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

internal class XaExternalAnonymiserTests
{
    [Test]
    public void Anonymise_HappyPath_IsOk()
    {
        // Skip on Windows as bash scripts won't execute
        if (OperatingSystem.IsWindows())
            Assert.Ignore("Test requires bash and is not supported on Windows");

        // Arrange

        // Create a simple test script that copies the input to output
        using var tempDir = new DisposableTempDir();
        var scriptPath = Path.Combine(tempDir, "xa-anon-tool.sh");
        
        // Create a simple bash script that copies input to output (simulating anonymization)
        File.WriteAllText(scriptPath, "#!/bin/bash\ncp \"$1\" \"$2\"\nexit 0\n");
        
        // Make the script executable on Unix-like systems
        if (!OperatingSystem.IsWindows())
        {
            var chmod = new System.Diagnostics.Process
            {
                StartInfo = new System.Diagnostics.ProcessStartInfo
                {
                    FileName = "chmod",
                    Arguments = $"+x \"{scriptPath}\"",
                    UseShellExecute = false,
                }
            };
            chmod.Start();
            chmod.WaitForExit();
        }

        var globals = new GlobalOptionsFactory().Load(nameof(Anonymise_HappyPath_IsOk));
        globals.DicomAnonymiserOptions!.XaAnonymiserToolPath = scriptPath;

        var ds = new DicomDataset
        {
            { DicomTag.SOPClassUID, DicomUID.XRayAngiographicImageStorage },
            { DicomTag.StudyInstanceUID, "1" },
            { DicomTag.SeriesInstanceUID, "2" },
            { DicomTag.SOPInstanceUID, "3" },
        };
        var srcDcm = new DicomFile(ds);

        var srcPath = Path.Combine(tempDir, "in.dcm");
        srcDcm.Save(srcPath);

        var fileSystem = new FileSystem();
        var srcFile = fileSystem.FileInfo.New(srcPath);
        var destPath = Path.Combine(tempDir, "out.dcm");
        var destFile = fileSystem.FileInfo.New(destPath);

        using var anonymiser = new XaExternalAnonymiser(globals);

        // Act

        var status = anonymiser.Anonymise(srcFile, destFile, "XA", out var message);

        // Assert

        Assert.Multiple(() =>
        {
            Assert.That(status, Is.EqualTo(ExtractedFileStatus.Anonymised));
            Assert.That(message, Is.Null);
            Assert.That(File.Exists(destPath), Is.True);
        });
    }

    [Test]
    public void Anonymise_ToolNotFound_ReturnsError()
    {
        // Arrange

        var globals = new GlobalOptionsFactory().Load(nameof(Anonymise_ToolNotFound_ReturnsError));
        globals.DicomAnonymiserOptions!.XaAnonymiserToolPath = "/nonexistent/tool.sh";

        // Act & Assert

        Assert.Throws<System.ArgumentException>(() => new XaExternalAnonymiser(globals));
    }

    [Test]
    public void Anonymise_ToolPathNotSet_ThrowsException()
    {
        // Arrange

        var globals = new GlobalOptionsFactory().Load(nameof(Anonymise_ToolPathNotSet_ThrowsException));
        globals.DicomAnonymiserOptions!.XaAnonymiserToolPath = null;

        // Act & Assert

        Assert.Throws<System.ArgumentException>(() => new XaExternalAnonymiser(globals));
    }

    [Test]
    public void Anonymise_ToolFailsWithExitCode_ReturnsError()
    {
        // Skip on Windows as bash scripts won't execute
        if (OperatingSystem.IsWindows())
            Assert.Ignore("Test requires bash and is not supported on Windows");

        // Arrange

        using var tempDir = new DisposableTempDir();
        var scriptPath = Path.Combine(tempDir, "xa-anon-tool-fail.sh");
        
        // Create a script that fails
        File.WriteAllText(scriptPath, "#!/bin/bash\nexit 1\n");
        
        if (!OperatingSystem.IsWindows())
        {
            var chmod = new System.Diagnostics.Process
            {
                StartInfo = new System.Diagnostics.ProcessStartInfo
                {
                    FileName = "chmod",
                    Arguments = $"+x \"{scriptPath}\"",
                    UseShellExecute = false,
                }
            };
            chmod.Start();
            chmod.WaitForExit();
        }

        var globals = new GlobalOptionsFactory().Load(nameof(Anonymise_ToolFailsWithExitCode_ReturnsError));
        globals.DicomAnonymiserOptions!.XaAnonymiserToolPath = scriptPath;

        var ds = new DicomDataset
        {
            { DicomTag.SOPClassUID, DicomUID.XRayAngiographicImageStorage },
            { DicomTag.StudyInstanceUID, "1" },
            { DicomTag.SeriesInstanceUID, "2" },
            { DicomTag.SOPInstanceUID, "3" },
        };
        var srcDcm = new DicomFile(ds);

        var srcPath = Path.Combine(tempDir, "in.dcm");
        srcDcm.Save(srcPath);

        var fileSystem = new FileSystem();
        var srcFile = fileSystem.FileInfo.New(srcPath);
        var destPath = Path.Combine(tempDir, "out.dcm");
        var destFile = fileSystem.FileInfo.New(destPath);

        using var anonymiser = new XaExternalAnonymiser(globals);

        // Act

        var status = anonymiser.Anonymise(srcFile, destFile, "XA", out var message);

        // Assert

        Assert.Multiple(() =>
        {
            Assert.That(status, Is.EqualTo(ExtractedFileStatus.ErrorWontRetry));
            Assert.That(message, Does.Contain("exited with code 1"));
        });
    }
}
