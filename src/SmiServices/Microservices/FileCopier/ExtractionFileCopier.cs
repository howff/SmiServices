using NLog;
using SmiServices.Common.Messages;
using SmiServices.Common.Messages.Extraction;
using SmiServices.Common.Messaging;
using SmiServices.Common.Options;
using System;
using System.IO.Abstractions;
using System.Security.Cryptography;


namespace SmiServices.Microservices.FileCopier;

public class ExtractionFileCopier : IFileCopier
{
    private readonly FileCopierOptions _options;

    private readonly IProducerModel _copyStatusProducerModel;

    private readonly string _fileSystemRoot;
    private readonly string _extractionRoot;
    private readonly string? _poolRoot;
    private readonly IFileSystem _fileSystem;

    private readonly ILogger _logger;


    public ExtractionFileCopier(
        FileCopierOptions options,
        IProducerModel copyStatusCopyStatusProducerModel,
        string fileSystemRoot,
        string extractionRoot,
        IFileSystem? fileSystem = null,
        string? poolRoot = null)
    {
        _options = options;
        _copyStatusProducerModel = copyStatusCopyStatusProducerModel;
        _fileSystemRoot = fileSystemRoot;
        _extractionRoot = extractionRoot;
        _poolRoot = poolRoot;
        _fileSystem = fileSystem ?? new FileSystem();

        if (!_fileSystem.Directory.Exists(_fileSystemRoot))
            throw new ArgumentException($"Cannot find the specified fileSystemRoot: '{_fileSystemRoot}'");
        if (!_fileSystem.Directory.Exists(_extractionRoot))
            throw new ArgumentException($"Cannot find the specified extractionRoot: '{_extractionRoot}'");
        if (_poolRoot != null && !_fileSystem.Directory.Exists(_poolRoot))
            throw new ArgumentException($"Cannot find the specified poolRoot: '{_poolRoot}'");

        _logger = LogManager.GetLogger(GetType().Name);
        _logger.Info($"fileSystemRoot={_fileSystemRoot}, extractionRoot={_extractionRoot}, poolRoot={_poolRoot ?? "null"}");
    }

    public void ProcessMessage(
        ExtractFileMessage message,
        IMessageHeader header)
    {
        string fullSrc = _fileSystem.Path.Combine(_fileSystemRoot, message.DicomFilePath);

        ExtractedFileStatusMessage statusMessage;

        if (!_fileSystem.File.Exists(fullSrc))
        {
            statusMessage = new ExtractedFileStatusMessage(message)
            {
                DicomFilePath = message.DicomFilePath,
                Status = ExtractedFileStatus.FileMissing,
                StatusMessage = $"Could not find '{fullSrc}'"
            };
            _ = _copyStatusProducerModel.SendMessage(statusMessage, header, _options.NoVerifyRoutingKey);
            return;
        }

        string fullDest = _fileSystem.Path.Combine(_extractionRoot, message.ExtractionDirectory, message.OutputPath);

        IDirectoryInfo parent = _fileSystem.Directory.GetParent(fullDest)
            ?? throw new ArgumentException($"Parameter {fullDest} is the filesystem root");

        if (!parent.Exists)
        {
            _logger.Debug($"Creating directory '{parent}'");
            parent.Create();
        }

        // Handle pooled extraction
        if (message.IsPooledExtraction && _poolRoot != null)
        {
            ProcessPooledExtraction(fullSrc, fullDest, message);
        }
        else
        {
            // Normal extraction - copy file directly
            if (_fileSystem.File.Exists(fullDest))
                _logger.Warn($"Output file '{fullDest}' already exists. Will overwrite.");

            _logger.Debug($"Copying source file to '{message.OutputPath}'");
            _fileSystem.File.Copy(fullSrc, fullDest, overwrite: true);
        }

        statusMessage = new ExtractedFileStatusMessage(message)
        {
            DicomFilePath = message.DicomFilePath,
            Status = ExtractedFileStatus.Copied,
            OutputFilePath = message.OutputPath,
        };
        _ = _copyStatusProducerModel.SendMessage(statusMessage, header, _options.NoVerifyRoutingKey);
    }

    private void ProcessPooledExtraction(string fullSrc, string fullDest, ExtractFileMessage message)
    {
        // Compute hash of source file to determine pool filename
        string poolFileName = ComputeFileHash(fullSrc);
        string poolFilePath = _fileSystem.Path.Combine(_poolRoot!, poolFileName);

        // If file doesn't exist in pool, copy it there
        if (!_fileSystem.File.Exists(poolFilePath))
        {
            _logger.Debug($"File not in pool. Copying '{fullSrc}' to pool as '{poolFileName}'");
            _fileSystem.File.Copy(fullSrc, poolFilePath, overwrite: false);
        }
        else
        {
            _logger.Debug($"File already exists in pool as '{poolFileName}'. Skipping copy.");
        }

        // Remove destination if it already exists (can't create symlink to existing file)
        if (_fileSystem.File.Exists(fullDest))
        {
            _logger.Debug($"Removing existing destination file '{fullDest}'");
            _fileSystem.File.Delete(fullDest);
        }

        // Create symbolic link from destination to pool file
        _logger.Debug($"Creating symbolic link from '{fullDest}' to '{poolFilePath}'");
        _fileSystem.File.CreateSymbolicLink(fullDest, poolFilePath);
    }

    private string ComputeFileHash(string filePath)
    {
        using var stream = _fileSystem.File.OpenRead(filePath);
        using var sha256 = SHA256.Create();
        byte[] hashBytes = sha256.ComputeHash(stream);
        return BitConverter.ToString(hashBytes).Replace("-", "").ToLowerInvariant();
    }
}
