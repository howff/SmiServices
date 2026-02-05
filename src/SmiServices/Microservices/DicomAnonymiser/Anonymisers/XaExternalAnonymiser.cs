using NLog;
using SmiServices.Common.Messages.Extraction;
using SmiServices.Common.Options;
using SmiServices.Microservices.DicomAnonymiser.Helpers;
using System;
using System.Diagnostics;
using System.IO;
using System.IO.Abstractions;

namespace SmiServices.Microservices.DicomAnonymiser.Anonymisers;

/// <summary>
/// Anonymiser for XA modality that calls an external program to produce anonymized DICOM files
/// </summary>
public class XaExternalAnonymiser : IDicomAnonymiser, IDisposable
{
    private readonly ILogger _logger = LogManager.GetCurrentClassLogger();
    private readonly string _xaToolPath;
    private readonly TimeSpan _timeout = TimeSpan.FromSeconds(60);

    public XaExternalAnonymiser(GlobalOptions globalOptions)
    {
        var dicomAnonymiserOptions = globalOptions.DicomAnonymiserOptions ?? throw new ArgumentException($"{nameof(globalOptions.DicomAnonymiserOptions)} was null", nameof(globalOptions));

        if (string.IsNullOrWhiteSpace(dicomAnonymiserOptions.XaAnonymiserToolPath))
            throw new ArgumentException($"{nameof(dicomAnonymiserOptions.XaAnonymiserToolPath)} was not set", nameof(globalOptions));

        if (!File.Exists(dicomAnonymiserOptions.XaAnonymiserToolPath))
            throw new ArgumentException($"{nameof(dicomAnonymiserOptions.XaAnonymiserToolPath)} '{dicomAnonymiserOptions.XaAnonymiserToolPath}' does not exist", nameof(globalOptions));

        _xaToolPath = dicomAnonymiserOptions.XaAnonymiserToolPath;
        _logger.Info($"XaExternalAnonymiser initialized with tool: {_xaToolPath}");
    }

    public ExtractedFileStatus Anonymise(IFileInfo sourceFile, IFileInfo destFile, string modality, out string? anonymiserStatusMessage)
    {
        _logger.Debug($"Anonymising XA file: {sourceFile.FullName} -> {destFile.FullName}");

        var args = $"\"{sourceFile.FullName}\" \"{destFile.FullName}\"";
        
        using var process = ProcessWrapper.CreateProcess(_xaToolPath, args);
        
        var outputBuilder = new System.Text.StringBuilder();
        var errorBuilder = new System.Text.StringBuilder();

        process.OutputDataReceived += (_, e) =>
        {
            if (!string.IsNullOrEmpty(e.Data))
            {
                _logger.Debug($"[xa-tool stdout] {e.Data}");
                outputBuilder.AppendLine(e.Data);
            }
        };

        process.ErrorDataReceived += (_, e) =>
        {
            if (!string.IsNullOrEmpty(e.Data))
            {
                _logger.Debug($"[xa-tool stderr] {e.Data}");
                errorBuilder.AppendLine(e.Data);
            }
        };

        try
        {
            process.Start();
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();

            if (!process.WaitForExit((int)_timeout.TotalMilliseconds))
            {
                process.Kill();
                anonymiserStatusMessage = $"XA anonymiser tool timed out after {_timeout.TotalSeconds}s";
                _logger.Error(anonymiserStatusMessage);
                return ExtractedFileStatus.ErrorWontRetry;
            }

            process.WaitForExit(); // Ensure async output is complete

            if (process.ExitCode != 0)
            {
                anonymiserStatusMessage = $"XA anonymiser tool exited with code {process.ExitCode}. Error: {errorBuilder}";
                _logger.Error(anonymiserStatusMessage);
                return ExtractedFileStatus.ErrorWontRetry;
            }

            if (!File.Exists(destFile.FullName))
            {
                anonymiserStatusMessage = $"XA anonymiser tool completed but output file was not created: {destFile.FullName}";
                _logger.Error(anonymiserStatusMessage);
                return ExtractedFileStatus.ErrorWontRetry;
            }

            _logger.Debug($"XA anonymisation completed successfully: {destFile.FullName}");
            anonymiserStatusMessage = null;
            return ExtractedFileStatus.Anonymised;
        }
        catch (Exception ex)
        {
            anonymiserStatusMessage = $"Exception during XA anonymisation: {ex.Message}";
            _logger.Error(ex, anonymiserStatusMessage);
            return ExtractedFileStatus.ErrorWontRetry;
        }
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        // No persistent resources to dispose
    }
}
