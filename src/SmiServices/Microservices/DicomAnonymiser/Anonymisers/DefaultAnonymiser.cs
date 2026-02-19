using NLog;
using SmiServices.Common.Messages.Extraction;
using SmiServices.Common.Options;
using System;
using System.IO.Abstractions;

namespace SmiServices.Microservices.DicomAnonymiser.Anonymisers;

public class DefaultAnonymiser : IDicomAnonymiser, IDisposable
{
    private readonly ILogger _logger = LogManager.GetCurrentClassLogger();
    private readonly SmiCtpAnonymiser _ctpAnonymiser;
    private readonly XaExternalAnonymiser? _xaAnonymiser;

    public DefaultAnonymiser(GlobalOptions globalOptions)
    {
        var dicomAnonymiserOptions = globalOptions.DicomAnonymiserOptions!;

        _ctpAnonymiser = new SmiCtpAnonymiser(globalOptions);

        // Initialize XA anonymiser if tool path is configured
        if (!string.IsNullOrWhiteSpace(dicomAnonymiserOptions.XaAnonymiserToolPath))
        {
            _xaAnonymiser = new XaExternalAnonymiser(globalOptions);
        }
    }

    /// <summary>
    /// Anonymises a DICOM file based on image modality
    /// </summary>
    public ExtractedFileStatus Anonymise(IFileInfo sourceFile, IFileInfo destFile, string modality, out string? anonymiserStatusMessage)
    {
        // Route XA modality to external anonymiser if configured
        if (modality == "XA" && _xaAnonymiser != null)
        {
            _logger.Debug($"Routing XA modality to external anonymiser");
            return _xaAnonymiser.Anonymise(sourceFile, destFile, modality, out anonymiserStatusMessage);
        }

        var status = _ctpAnonymiser.Anonymise(sourceFile, destFile, modality, out string? ctpStatusMessage);
        if (status != ExtractedFileStatus.Anonymised)
        {
            anonymiserStatusMessage = ctpStatusMessage;
            return status;
        }

        // TODO(rkm 2024-12-17) Implement SR anon here (instead of via CTP), and add pixel anonymiser

        anonymiserStatusMessage = null;
        return ExtractedFileStatus.Anonymised;
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        _ctpAnonymiser.Dispose();
        _xaAnonymiser?.Dispose();
    }
}
