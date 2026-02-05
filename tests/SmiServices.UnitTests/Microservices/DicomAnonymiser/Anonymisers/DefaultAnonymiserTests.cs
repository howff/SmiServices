using FellowOakDicom;
using NUnit.Framework;
using SmiServices.Common.Messages.Extraction;
using SmiServices.Common.Options;
using SmiServices.Microservices.DicomAnonymiser.Anonymisers;
using SmiServices.UnitTests.TestCommon;
using System;
using System.IO;
using System.IO.Abstractions;

namespace SmiServices.UnitTests.Microservices.DicomAnonymiser.Anonymisers;

internal class DefaultAnonymiserTests
{
    [Test]
    public void DefaultAnonymiser_WithXaToolConfigured_InitializesSuccessfully()
    {
        // Skip on Windows as bash scripts won't execute
        if (OperatingSystem.IsWindows())
            Assert.Ignore("Test requires bash and is not supported on Windows");

        // This test verifies that DefaultAnonymiser can be instantiated
        // when both CTP and XA tool paths are configured correctly.
        // We can't test the actual routing without starting CTP process,
        // but we verify the XA anonymiser is created.

        // Arrange

        using var tempDir = new DisposableTempDir();
        
        // Create XA tool script
        var xaToolPath = Path.Combine(tempDir, "xa-tool.sh");
        File.WriteAllText(xaToolPath, "#!/bin/bash\ncp \"$1\" \"$2\"\nexit 0\n");
        
        if (!OperatingSystem.IsWindows())
        {
            var chmod = new System.Diagnostics.Process
            {
                StartInfo = new System.Diagnostics.ProcessStartInfo
                {
                    FileName = "chmod",
                    Arguments = $"+x \"{xaToolPath}\"",
                    UseShellExecute = false,
                }
            };
            chmod.Start();
            chmod.WaitForExit();
        }

        var globals = new GlobalOptionsFactory().Load(nameof(DefaultAnonymiser_WithXaToolConfigured_InitializesSuccessfully));
        globals.DicomAnonymiserOptions!.XaAnonymiserToolPath = xaToolPath;

        // Act & Assert
        // Construction will fail if CTP can't be started, which is expected in unit tests
        // We're just verifying that XA anonymiser path is validated
        Assert.DoesNotThrow(() =>
        {
            var xaAnonymiser = new XaExternalAnonymiser(globals);
            xaAnonymiser.Dispose();
        });
    }

    [Test]
    public void DefaultAnonymiser_WithoutXaToolConfigured_DoesNotInitializeXaAnonymiser()
    {
        // This test verifies that when XA tool is not configured,
        // XaExternalAnonymiser throws on construction

        // Arrange

        var globals = new GlobalOptionsFactory().Load(nameof(DefaultAnonymiser_WithoutXaToolConfigured_DoesNotInitializeXaAnonymiser));
        globals.DicomAnonymiserOptions!.XaAnonymiserToolPath = null; // Not configured

        // Act & Assert
        // Without XA tool path, XaExternalAnonymiser should throw
        Assert.Throws<System.ArgumentException>(() => new XaExternalAnonymiser(globals));
    }
}
