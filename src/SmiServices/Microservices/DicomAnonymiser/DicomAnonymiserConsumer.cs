using NLog;
using SmiServices.Common.Messages;
using SmiServices.Common.Messages.Extraction;
using SmiServices.Common.Messaging;
using SmiServices.Common.Options;
using SmiServices.Microservices.DicomAnonymiser.Anonymisers;
using System;
using System.IO.Abstractions;
using System.Security.Cryptography;

namespace SmiServices.Microservices.DicomAnonymiser;

public class DicomAnonymiserConsumer : Consumer<ExtractFileMessage>
{
    private readonly ILogger _logger = LogManager.GetCurrentClassLogger();
    private readonly IFileSystem _fileSystem;
    private readonly DicomAnonymiserOptions _options;
    private readonly string _fileSystemRoot;
    private readonly string _extractRoot;
    private readonly string? _poolRoot;
    private readonly IDicomAnonymiser _anonymiser;
    private readonly IProducerModel _statusMessageProducer;

    public DicomAnonymiserConsumer(
        DicomAnonymiserOptions options,
        string fileSystemRoot,
        string extractRoot,
        IDicomAnonymiser anonymiser,
        IProducerModel statusMessageProducer,
        IFileSystem? fileSystem = null,
        string? poolRoot = null
    )
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _fileSystem = fileSystem ?? new FileSystem();
        _fileSystemRoot = fileSystemRoot ?? throw new ArgumentNullException(nameof(fileSystemRoot));
        _extractRoot = extractRoot ?? throw new ArgumentNullException(nameof(extractRoot));
        _poolRoot = poolRoot;
        _anonymiser = anonymiser ?? throw new ArgumentNullException(nameof(anonymiser));
        _statusMessageProducer = statusMessageProducer ?? throw new ArgumentNullException(nameof(statusMessageProducer));

        if (!_fileSystem.Directory.Exists(_fileSystemRoot))
            throw new Exception($"Filesystem root does not exist: '{fileSystemRoot}'");

        if (!_fileSystem.Directory.Exists(_extractRoot))
            throw new Exception($"Extract root does not exist: '{extractRoot}'");

        if (_poolRoot != null && !_fileSystem.Directory.Exists(_poolRoot))
            throw new Exception($"Pool root does not exist: '{poolRoot}'");
    }

    protected override void ProcessMessageImpl(IMessageHeader header, ExtractFileMessage message, ulong tag)
    {
        if (message.IsIdentifiableExtraction)
            throw new Exception("DicomAnonymiserConsumer should not handle identifiable extraction messages");

        var statusMessage = new ExtractedFileStatusMessage(message);

        var sourceFileAbs = _fileSystem.FileInfo.New(_fileSystem.Path.Combine(_fileSystemRoot, message.DicomFilePath));

        if (!sourceFileAbs.Exists)
        {
            statusMessage.Status = ExtractedFileStatus.FileMissing;
            statusMessage.StatusMessage = $"Could not find file to anonymise: '{sourceFileAbs}'";
            statusMessage.OutputFilePath = null;
            _statusMessageProducer.SendMessage(statusMessage, header, _options.RoutingKeyFailure);

            Ack(header, tag);
            return;
        }

        if (_options.FailIfSourceWriteable && !sourceFileAbs.Attributes.HasFlag(System.IO.FileAttributes.ReadOnly))
        {
            statusMessage.Status = ExtractedFileStatus.ErrorWontRetry;
            statusMessage.StatusMessage = $"Source file was writeable and FailIfSourceWriteable is set: '{sourceFileAbs}'";
            statusMessage.OutputFilePath = null;
            _statusMessageProducer.SendMessage(statusMessage, header, _options.RoutingKeyFailure);

            Ack(header, tag);
            return;
        }

        var extractionDirAbs = _fileSystem.Path.Combine(_extractRoot, message.ExtractionDirectory);

        // NOTE(rkm 2021-12-07) Since this directory should have already been created, we treat this more like an assertion and throw if not found.
        // This helps prevent a flood of messages if e.g. the filesystem is temporarily unavailable.
        if (!_fileSystem.Directory.Exists(extractionDirAbs))
            throw new System.IO.DirectoryNotFoundException($"Expected extraction directory to exist: '{extractionDirAbs}'");

        var destFileAbs = _fileSystem.FileInfo.New(_fileSystem.Path.Combine(extractionDirAbs, message.OutputPath));

        destFileAbs.Directory!.Create();

        ExtractedFileStatus anonymiserStatus;
        string? anonymiserStatusMessage;

        // Handle pooled extraction
        if (message.IsPooledExtraction && _poolRoot != null)
        {
            anonymiserStatus = ProcessPooledAnonymisation(sourceFileAbs, destFileAbs, message.Modality, out anonymiserStatusMessage);
        }
        else
        {
            // Normal anonymisation - write directly to destination
            _logger.Debug($"Anonymising '{sourceFileAbs}' to '{destFileAbs}'");
            anonymiserStatus = _anonymiser.Anonymise(sourceFileAbs, destFileAbs, message.Modality, out anonymiserStatusMessage);
        }

        var logMessage = $"Anonymisation of '{sourceFileAbs}' returned {anonymiserStatus}";
        if (anonymiserStatus != ExtractedFileStatus.Anonymised)
            logMessage += $" with message {anonymiserStatusMessage}";
        _logger.Info(logMessage);

        statusMessage.Status = anonymiserStatus;
        statusMessage.StatusMessage = anonymiserStatusMessage;

        string routingKey;

        if (anonymiserStatus == ExtractedFileStatus.Anonymised)
        {
            routingKey = _options.RoutingKeySuccess ?? "verify";
        }
        else
        {
            statusMessage.OutputFilePath = null;
            routingKey = _options.RoutingKeyFailure ?? "noverify";
        }

        _statusMessageProducer.SendMessage(statusMessage, header, routingKey);

        Ack(header, tag);
    }

    private ExtractedFileStatus ProcessPooledAnonymisation(IFileInfo sourceFileAbs, IFileInfo destFileAbs, string modality, out string? statusMessage)
    {
        // Create a temporary file for anonymisation
        var tempFileName = _fileSystem.Path.Combine(_fileSystem.Path.GetTempPath(), _fileSystem.Path.GetRandomFileName());
        var tempFileInfo = _fileSystem.FileInfo.New(tempFileName);

        try
        {
            // Anonymise to temporary file
            _logger.Debug($"Anonymising '{sourceFileAbs}' to temporary file '{tempFileName}'");
            var anonymiserStatus = _anonymiser.Anonymise(sourceFileAbs, tempFileInfo, modality, out statusMessage);

            if (anonymiserStatus != ExtractedFileStatus.Anonymised)
            {
                // Anonymisation failed, clean up temp file and return
                if (_fileSystem.File.Exists(tempFileName))
                    _fileSystem.File.Delete(tempFileName);
                return anonymiserStatus;
            }

            // Compute hash of anonymised file
            string poolFileName = ComputeFileHash(tempFileName);
            string poolFilePath = _fileSystem.Path.Combine(_poolRoot!, poolFileName);

            // Check if anonymised file already exists in pool
            if (!_fileSystem.File.Exists(poolFilePath))
            {
                _logger.Debug($"Anonymised file not in pool. Moving to pool as '{poolFileName}'");
                _fileSystem.File.Move(tempFileName, poolFilePath, overwrite: false);
            }
            else
            {
                _logger.Debug($"Anonymised file already exists in pool as '{poolFileName}'. Deleting temp file.");
                _fileSystem.File.Delete(tempFileName);
            }

            // Remove destination if it already exists (can't create symlink to existing file)
            if (_fileSystem.File.Exists(destFileAbs.FullName))
            {
                _logger.Debug($"Removing existing destination file '{destFileAbs.FullName}'");
                _fileSystem.File.Delete(destFileAbs.FullName);
            }

            // Create symbolic link from destination to pool file
            _logger.Debug($"Creating symbolic link from '{destFileAbs.FullName}' to '{poolFilePath}'");
            _fileSystem.File.CreateSymbolicLink(destFileAbs.FullName, poolFilePath);

            return ExtractedFileStatus.Anonymised;
        }
        catch
        {
            // Clean up temp file on any error
            if (_fileSystem.File.Exists(tempFileName))
                _fileSystem.File.Delete(tempFileName);
            throw;
        }
    }

    private string ComputeFileHash(string filePath)
    {
        using var stream = _fileSystem.File.OpenRead(filePath);
        using var sha256 = SHA256.Create();
        byte[] hashBytes = sha256.ComputeHash(stream);
        return BitConverter.ToString(hashBytes).Replace("-", "").ToLowerInvariant();
    }
}
