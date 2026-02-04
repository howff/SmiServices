using FellowOakDicom;
using Moq;
using NUnit.Framework;
using SmiServices.Common.Events;
using SmiServices.Common.Messages;
using SmiServices.Common.Messages.Extraction;
using SmiServices.Common.Messaging;
using SmiServices.Common.Options;
using SmiServices.Microservices.DicomAnonymiser;
using SmiServices.Microservices.DicomAnonymiser.Anonymisers;
using SmiServices.UnitTests.TestCommon;
using System;
using System.IO;
using System.IO.Abstractions;
using System.IO.Abstractions.TestingHelpers;
using System.Linq.Expressions;

namespace SmiServices.UnitTests.Microservices.DicomAnonymiser;

public class DicomAnonymiserConsumerTests
{
    #region Fixture Methods

    private MockFileSystem _mockFs = null!;
    private IDirectoryInfo _dicomRootDirInfo = null!;
    private IDirectoryInfo _extractRootDirInfo = null!;
    private string _extractDir = null!;
    private string _sourceDcmPathAbs = null!;
    private ExtractFileMessage _extractFileMessage = null!;
    private DicomAnonymiserOptions _options = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() { }

    #endregion

    #region Test Methods

    [SetUp]
    public void SetUp()
    {
        _mockFs = new MockFileSystem();

        _dicomRootDirInfo = _mockFs.Directory.CreateDirectory("dicom");
        _extractRootDirInfo = _mockFs.Directory.CreateDirectory("extract");

        var extractDirName = "extractDir";
        _extractDir = _mockFs.Path.Combine(_extractRootDirInfo.FullName, extractDirName);
        _mockFs.Directory.CreateDirectory(_extractDir);

        _sourceDcmPathAbs = _mockFs.Path.Combine(_dicomRootDirInfo.FullName, "foo.dcm");

        var dicomFile = new DicomFile();
        dicomFile.Dataset.Add(DicomTag.PatientID, "12345678");
        dicomFile.Dataset.Add(DicomTag.Modality, "CT");
        dicomFile.Dataset.Add(DicomTag.StudyInstanceUID, DicomUIDGenerator.GenerateDerivedFromUUID());
        dicomFile.Dataset.Add(DicomTag.SeriesInstanceUID, DicomUIDGenerator.GenerateDerivedFromUUID());
        dicomFile.Dataset.Add(DicomTag.SOPInstanceUID, DicomUIDGenerator.GenerateDerivedFromUUID());
        dicomFile.FileMetaInfo.MediaStorageSOPClassUID = DicomUID.SecondaryCaptureImageStorage;
        dicomFile.FileMetaInfo.MediaStorageSOPInstanceUID = DicomUIDGenerator.GenerateDerivedFromUUID();
        dicomFile.FileMetaInfo.ImplementationClassUID = DicomUIDGenerator.GenerateDerivedFromUUID();
        dicomFile.FileMetaInfo.TransferSyntax = DicomTransferSyntax.ExplicitVRLittleEndian;

        using var stream = new MemoryStream();
        dicomFile.Save(stream);

        var dicomBytes = stream.ToArray();
        _mockFs.AddFile(_sourceDcmPathAbs, new MockFileData(dicomBytes));

        // _mockFs.File.Create(_sourceDcmPathAbs).Dispose();
        _mockFs.File.SetAttributes(_sourceDcmPathAbs, _mockFs.File.GetAttributes(_sourceDcmPathAbs) | FileAttributes.ReadOnly);

        _extractFileMessage = new ExtractFileMessage
        {
            JobSubmittedAt = DateTime.UtcNow,
            ExtractionJobIdentifier = Guid.NewGuid(),
            ProjectNumber = "1234",
            ExtractionDirectory = extractDirName,
            DicomFilePath = "foo.dcm",
            OutputPath = "foo-an.dcm",
            Modality = "CT",
        };

        _options = new DicomAnonymiserOptions
        {
            RoutingKeySuccess = "yay",
            FailIfSourceWriteable = true,
            RoutingKeyFailure = "nay"
        };

        Console.WriteLine($"_dicomRootDirInfo.FullName: {_dicomRootDirInfo.FullName}");
        Console.WriteLine($"_extractRootDirInfo.FullName: {_extractRootDirInfo.FullName}");
        Console.WriteLine($"_extractDir: {_extractDir}");
        Console.WriteLine($"_sourceDcmPathAbs: {_sourceDcmPathAbs}");
    }

    private DicomAnonymiserConsumer GetNewDicomAnonymiserConsumer(
        IDicomAnonymiser? mockDicomAnonymiser = null,
        IProducerModel? mockProducerModel = null,
        string? poolRoot = null
    )
    {
        var consumer = new DicomAnonymiserConsumer(
            _options,
            _dicomRootDirInfo.FullName,
            _extractRootDirInfo.FullName,
            mockDicomAnonymiser ?? new Mock<IDicomAnonymiser>(MockBehavior.Strict).Object,
            mockProducerModel ?? new Mock<IProducerModel>(MockBehavior.Strict).Object,
            _mockFs,
            poolRoot
        );
        return consumer;
    }

    [TearDown]
    public void TearDown() { }

    #endregion

    #region Tests

    [Test]
    public void ProcessMessageImpl_HappyPath()
    {
        // Arrange

        Expression<Func<IDicomAnonymiser, ExtractedFileStatus>> expectedAnonCall =
            x => x.Anonymise(
                It.Is<IFileInfo>(x => x.FullName == _sourceDcmPathAbs),
                It.Is<IFileInfo>(x => x.FullName == _mockFs.Path.Combine(_extractDir, _extractFileMessage.OutputPath)),
                "CT",
                out It.Ref<string?>.IsAny
            );

        var mockAnonymiser = new Mock<IDicomAnonymiser>(MockBehavior.Strict);
        mockAnonymiser
            .Setup(expectedAnonCall)
            .Returns(ExtractedFileStatus.Anonymised);

        Expression<Func<IProducerModel, IMessageHeader>> expectedSendCall =
            x => x.SendMessage(
                It.Is<ExtractedFileStatusMessage>(x =>
                    x.Status == ExtractedFileStatus.Anonymised &&
                    x.StatusMessage == null &&
                    x.OutputFilePath == _extractFileMessage.OutputPath
                 ),
                It.IsAny<IMessageHeader>(),
                _options.RoutingKeySuccess
            );

        var mockProducerModel = new Mock<IProducerModel>();
        mockProducerModel.Setup(expectedSendCall);

        var consumer = GetNewDicomAnonymiserConsumer(mockAnonymiser.Object, mockProducerModel.Object);

        // Act

        consumer.ProcessMessage(new MessageHeader(), _extractFileMessage, 1);

        // Assert

        TestTimelineAwaiter.Await(() => consumer.AckCount == 1 && consumer.NackCount == 0);

        mockAnonymiser.Verify(expectedAnonCall, Times.Once);
        mockProducerModel.Verify(expectedSendCall, Times.Once);
    }

    [Test]
    public void ProcessMessageImpl_IsIdentifiableExtraction_ThrowsException()
    {
        // Arrange

        _extractFileMessage.IsIdentifiableExtraction = true;

        var consumer = GetNewDicomAnonymiserConsumer();

        FatalErrorEventArgs? fatalArgs = null;
        consumer.OnFatal += (_, args) => fatalArgs = args;

        // Act

        consumer.ProcessMessage(new MessageHeader(), _extractFileMessage, 1);

        // Assert

        TestTimelineAwaiter.Await(() => fatalArgs != null, "Expected Fatal to be called");
        Assert.Multiple(() =>
        {
            Assert.That(fatalArgs?.Message, Is.EqualTo("ProcessMessageImpl threw unhandled exception"));
            Assert.That(fatalArgs!.Exception!.Message, Is.EqualTo("DicomAnonymiserConsumer should not handle identifiable extraction messages"));
            Assert.That(consumer.AckCount, Is.EqualTo(0));
            Assert.That(consumer.NackCount, Is.EqualTo(0));
        });
    }

    [Test]
    public void ProcessMessageImpl_SourceFileMissing_AcksWithFailureStatus()
    {
        // Arrange

        _mockFs.File.SetAttributes(_sourceDcmPathAbs, _mockFs.File.GetAttributes(_sourceDcmPathAbs) & ~FileAttributes.ReadOnly);
        _mockFs.File.Delete(_sourceDcmPathAbs);

        Expression<Func<IProducerModel, IMessageHeader>> expectedCall =
            x => x.SendMessage(
                It.Is<ExtractedFileStatusMessage>(x =>
                    x.Status == ExtractedFileStatus.FileMissing &&
                    x.StatusMessage == $"Could not find file to anonymise: '{_sourceDcmPathAbs}'" &&
                    x.OutputFilePath == null
                 ),
                It.IsAny<IMessageHeader>(),
                _options.RoutingKeyFailure
            );

        var mockProducerModel = new Mock<IProducerModel>();
        mockProducerModel.Setup(expectedCall);

        var consumer = GetNewDicomAnonymiserConsumer(null, mockProducerModel.Object);

        // Act

        consumer.ProcessMessage(new MessageHeader(), _extractFileMessage, 1);

        // Assert

        TestTimelineAwaiter.Await(() => consumer.AckCount == 1 && consumer.NackCount == 0);

        mockProducerModel.Verify(expectedCall, Times.Once);
    }

    [Test]
    public void ProcessMessageImpl_FailIfSourceWriteable_AcksWithFailureStatus()
    {
        // Arrange

        _mockFs.File.SetAttributes(_sourceDcmPathAbs, _mockFs.File.GetAttributes(_sourceDcmPathAbs) & ~FileAttributes.ReadOnly);

        Expression<Func<IProducerModel, IMessageHeader>> expectedCall =
            x => x.SendMessage(
                It.Is<ExtractedFileStatusMessage>(x =>
                    x.Status == ExtractedFileStatus.ErrorWontRetry &&
                    x.StatusMessage == $"Source file was writeable and FailIfSourceWriteable is set: '{_sourceDcmPathAbs}'" &&
                    x.OutputFilePath == null
                 ),
                It.IsAny<IMessageHeader>(),
                _options.RoutingKeyFailure
            );
        var mockProducerModel = new Mock<IProducerModel>();
        mockProducerModel.Setup(expectedCall);

        var consumer = GetNewDicomAnonymiserConsumer(null, mockProducerModel.Object);

        // Act

        consumer.ProcessMessage(new MessageHeader(), _extractFileMessage, 1);

        // Assert

        TestTimelineAwaiter.Await(() => consumer.AckCount == 1 && consumer.NackCount == 0);

        mockProducerModel.Verify(expectedCall, Times.Once);
    }

    [Test]
    public void ProcessMessageImpl_ExtractionDirMissing_ThrowsException()
    {
        // Arrange

        _mockFs.Directory.Delete(_extractDir);

        var consumer = GetNewDicomAnonymiserConsumer();

        FatalErrorEventArgs? fatalArgs = null;
        consumer.OnFatal += (_, args) => fatalArgs = args;

        // Act

        consumer.ProcessMessage(new MessageHeader(), _extractFileMessage, 1);

        // Assert

        TestTimelineAwaiter.Await(() => fatalArgs != null, "Expected Fatal to be called");

        Assert.Multiple(() =>
        {
            Assert.That(fatalArgs?.Message, Is.EqualTo("ProcessMessageImpl threw unhandled exception"));
            Assert.That(fatalArgs!.Exception!.Message, Is.EqualTo($"Expected extraction directory to exist: '{_extractDir}'"));
            Assert.That(consumer.AckCount, Is.EqualTo(0));
            Assert.That(consumer.NackCount, Is.EqualTo(0));
        });
    }

    private class FailingAnonymiser : IDicomAnonymiser
    {
        public ExtractedFileStatus Anonymise(IFileInfo sourceFile, IFileInfo destFile, string modality, out string? anonymiserStatusMessage)
        {
            anonymiserStatusMessage = "oh no!";
            return ExtractedFileStatus.ErrorWontRetry;
        }
    }

    [Test]
    public void ProcessMessageImpl_AnonymisationFailed_AcksWithFailureStatus()
    {
        // Arrange

        var anonymiser = new FailingAnonymiser();

        Expression<Func<IProducerModel, IMessageHeader>> expectedCall =
            x => x.SendMessage(
                It.Is<ExtractedFileStatusMessage>(x =>
                    x.Status == ExtractedFileStatus.ErrorWontRetry &&
                    x.StatusMessage!.StartsWith("oh no!") &&
                    x.OutputFilePath == null
                 ),
                It.IsAny<IMessageHeader>(),
                _options.RoutingKeyFailure
            );

        var mockProducerModel = new Mock<IProducerModel>();
        mockProducerModel.Setup(expectedCall);

        var consumer = GetNewDicomAnonymiserConsumer(anonymiser, mockProducerModel.Object);

        // Act
        consumer.ProcessMessage(new MessageHeader(), _extractFileMessage, 1);

        // Assert

        TestTimelineAwaiter.Await(() => consumer.AckCount == 1 && consumer.NackCount == 0);

        mockProducerModel.Verify(expectedCall, Times.Once);
    }

    [Test]
    public void ProcessMessageImpl_PooledExtraction_CreatesSymlinkToPool()
    {
        // Arrange
        const string poolRoot = "pool";
        _mockFs.Directory.CreateDirectory(poolRoot);

        _extractFileMessage.IsPooledExtraction = true;

        var mockAnonymiser = new Mock<IDicomAnonymiser>(MockBehavior.Strict);
        string? anonStatusMsg = null;
        mockAnonymiser
            .Setup(x => x.Anonymise(
                It.Is<IFileInfo>(f => f.FullName == _sourceDcmPathAbs),
                It.IsAny<IFileInfo>(),
                "CT",
                out anonStatusMsg
            ))
            .Callback((IFileInfo src, IFileInfo dest, string mod, out string? msg) =>
            {
                // Simulate anonymisation by creating a file
                _mockFs.File.WriteAllBytes(dest.FullName, [1, 2, 3, 4]);
                msg = null;
            })
            .Returns(ExtractedFileStatus.Anonymised);

        var mockProducerModel = new Mock<IProducerModel>();
        mockProducerModel
            .Setup(x => x.SendMessage(
                It.Is<ExtractedFileStatusMessage>(m => m.Status == ExtractedFileStatus.Anonymised),
                It.IsAny<IMessageHeader>(),
                _options.RoutingKeySuccess
            ));

        var consumer = new DicomAnonymiserConsumer(
            _options,
            _dicomRootDirInfo.FullName,
            _extractRootDirInfo.FullName,
            mockAnonymiser.Object,
            mockProducerModel.Object,
            _mockFs,
            poolRoot
        );

        // Act
        consumer.ProcessMessage(new MessageHeader(), _extractFileMessage, 1);

        // Assert
        TestTimelineAwaiter.Await(() => consumer.AckCount == 1 && consumer.NackCount == 0);

        // Verify destination is a symlink
        string expectedDest = _mockFs.Path.Combine(_extractDir, _extractFileMessage.OutputPath);
        var linkInfo = _mockFs.FileInfo.New(expectedDest);
        Assert.Multiple(() =>
        {
            Assert.That(linkInfo.Exists, Is.True, "Symlink should exist");
            Assert.That(linkInfo.LinkTarget, Is.Not.Null, "Should be a symbolic link");
        });

        // Verify the pool file exists
        string poolFilePath = linkInfo.LinkTarget!;
        Assert.That(_mockFs.File.Exists(poolFilePath), Is.True, "Pool file should exist");
    }

    [Test]
    public void ProcessMessageImpl_PooledExtraction_ReusesDuplicateFiles()
    {
        // Arrange
        const string poolRoot = "pool";
        _mockFs.Directory.CreateDirectory(poolRoot);

        _extractFileMessage.IsPooledExtraction = true;

        var mockAnonymiser = new Mock<IDicomAnonymiser>(MockBehavior.Strict);
        string? anonStatusMsg = null;
        mockAnonymiser
            .Setup(x => x.Anonymise(
                It.IsAny<IFileInfo>(),
                It.IsAny<IFileInfo>(),
                It.IsAny<string>(),
                out anonStatusMsg
            ))
            .Callback((IFileInfo src, IFileInfo dest, string mod, out string? msg) =>
            {
                // Simulate anonymisation by creating a file with the same content for the same source
                _mockFs.File.WriteAllBytes(dest.FullName, [1, 2, 3, 4]);
                msg = null;
            })
            .Returns(ExtractedFileStatus.Anonymised);

        var mockProducerModel = new Mock<IProducerModel>();
        mockProducerModel
            .Setup(x => x.SendMessage(
                It.IsAny<IMessage>(),
                It.IsAny<IMessageHeader>(),
                It.IsAny<string>()
            ));

        var consumer = new DicomAnonymiserConsumer(
            _options,
            _dicomRootDirInfo.FullName,
            _extractRootDirInfo.FullName,
            mockAnonymiser.Object,
            mockProducerModel.Object,
            _mockFs,
            poolRoot
        );

        // Act - Process first message
        consumer.ProcessMessage(new MessageHeader(), _extractFileMessage, 1);
        TestTimelineAwaiter.Await(() => consumer.AckCount == 1);

        // Get pool file count after first processing
        int poolFileCountBefore = _mockFs.Directory.GetFiles(poolRoot).Length;

        // Process second message with different output path
        var secondMessage = new ExtractFileMessage
        {
            JobSubmittedAt = DateTime.UtcNow,
            ExtractionJobIdentifier = Guid.NewGuid(),
            ProjectNumber = "5678",
            ExtractionDirectory = _extractFileMessage.ExtractionDirectory,
            DicomFilePath = _extractFileMessage.DicomFilePath,
            OutputPath = "different-an.dcm",
            Modality = "CT",
            IsPooledExtraction = true,
        };

        consumer.ProcessMessage(new MessageHeader(), secondMessage, 2);

        // Assert
        TestTimelineAwaiter.Await(() => consumer.AckCount == 2);

        // Get pool file count after second processing
        int poolFileCountAfter = _mockFs.Directory.GetFiles(poolRoot).Length;

        // Should be the same - no new file created (since both are anonymised versions of the same source)
        Assert.That(poolFileCountAfter, Is.EqualTo(poolFileCountBefore), 
            "No new pool file should be created for duplicate anonymised content");

        // Both destinations should exist and point to the same pool file
        string dest1 = _mockFs.Path.Combine(_extractDir, _extractFileMessage.OutputPath);
        string dest2 = _mockFs.Path.Combine(_extractDir, secondMessage.OutputPath);

        var link1 = _mockFs.FileInfo.New(dest1);
        var link2 = _mockFs.FileInfo.New(dest2);

        Assert.Multiple(() =>
        {
            Assert.That(link1.Exists, Is.True);
            Assert.That(link2.Exists, Is.True);
            Assert.That(link1.LinkTarget, Is.EqualTo(link2.LinkTarget), 
                "Both symlinks should point to the same pool file");
        });
    }

    #endregion
}
