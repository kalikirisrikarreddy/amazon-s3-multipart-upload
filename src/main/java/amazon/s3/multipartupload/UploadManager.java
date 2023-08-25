package amazon.s3.multipartupload;

import java.io.File;
import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.exc.StreamWriteException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.github.javafaker.Faker;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AccelerateConfiguration;
import software.amazon.awssdk.services.s3.model.BucketAccelerateStatus;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutBucketAccelerateConfigurationRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

public class UploadManager {

	static S3Client s3ClientWithTransferAccelerationEnabled = S3Client.builder().region(Region.US_EAST_1)
			.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("DUMMY", "DUMMY")))
			.accelerate(true).build();
	static S3Client s3Client = S3Client.builder().region(Region.US_EAST_1)
			.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("DUMMY", "DUMMY")))
			.build();
	static String bucketNamePrefix = "YOUR_OWN_UNIQUE_BUCKET_NAME";
	static String plainBucket = bucketNamePrefix + "-plain";
	static String transferAcceleratedBucket = bucketNamePrefix + "-transfer-accelerated";

	public static void main(String[] args)
			throws StreamWriteException, DatabindException, IOException, InterruptedException, ExecutionException {
		generateFilesWithFakeData();
		createPlainAndTransferAcceleratedBucket();
		System.out.println("System's timezone : " + ZoneId.systemDefault().getId());
		uploadObjectUsingMultipartUploadWithoutTransferAcceleration();
		System.out.println(
				"----------------------------------------------------------------------------------------------------------------------");
		uploadObjectUsingMultipartUploadWithTransferAcceleration();
	}

	private static void uploadObjectUsingMultipartUploadWithTransferAcceleration() throws InterruptedException {
		Map<String, Integer> eTagToPartNumberMapping = new ConcurrentHashMap<>();
		long startTime = System.currentTimeMillis();
		CreateMultipartUploadResponse createMultipartUploadResponse = s3ClientWithTransferAccelerationEnabled
				.createMultipartUpload(CreateMultipartUploadRequest.builder().bucket(transferAcceleratedBucket)
						.key("bigfile.csv").build());
		String uploadId = createMultipartUploadResponse.uploadId();

		UploadPartResponse uploadPartResponse = s3ClientWithTransferAccelerationEnabled
				.uploadPart(
						UploadPartRequest.builder().bucket(transferAcceleratedBucket).key("bigfile.csv")
								.uploadId(uploadId).partNumber(1).build(),
						RequestBody.fromFile(new File("smallfile1.csv")));
		eTagToPartNumberMapping.put(uploadPartResponse.eTag(), 1);

		uploadPartResponse = s3ClientWithTransferAccelerationEnabled
				.uploadPart(
						UploadPartRequest.builder().bucket(transferAcceleratedBucket).key("bigfile.csv")
								.uploadId(uploadId).partNumber(2).build(),
						RequestBody.fromFile(new File("smallfile2.csv")));
		eTagToPartNumberMapping.put(uploadPartResponse.eTag(), 2);

		uploadPartResponse = s3ClientWithTransferAccelerationEnabled
				.uploadPart(
						UploadPartRequest.builder().bucket(transferAcceleratedBucket).key("bigfile.csv")
								.uploadId(uploadId).partNumber(3).build(),
						RequestBody.fromFile(new File("smallfile3.csv")));
		eTagToPartNumberMapping.put(uploadPartResponse.eTag(), 3);

		uploadPartResponse = s3ClientWithTransferAccelerationEnabled
				.uploadPart(
						UploadPartRequest.builder().bucket(transferAcceleratedBucket).key("bigfile.csv")
								.uploadId(uploadId).partNumber(4).build(),
						RequestBody.fromFile(new File("smallfile4.csv")));
		eTagToPartNumberMapping.put(uploadPartResponse.eTag(), 4);

		uploadPartResponse = s3ClientWithTransferAccelerationEnabled
				.uploadPart(
						UploadPartRequest.builder().bucket(transferAcceleratedBucket).key("bigfile.csv")
								.uploadId(uploadId).partNumber(5).build(),
						RequestBody.fromFile(new File("smallfile5.csv")));
		eTagToPartNumberMapping.put(uploadPartResponse.eTag(), 5);

		uploadPartResponse = s3ClientWithTransferAccelerationEnabled
				.uploadPart(
						UploadPartRequest.builder().bucket(transferAcceleratedBucket).key("bigfile.csv")
								.uploadId(uploadId).partNumber(6).build(),
						RequestBody.fromFile(new File("smallfile6.csv")));
		eTagToPartNumberMapping.put(uploadPartResponse.eTag(), 6);

		uploadPartResponse = s3ClientWithTransferAccelerationEnabled
				.uploadPart(
						UploadPartRequest.builder().bucket(transferAcceleratedBucket).key("bigfile.csv")
								.uploadId(uploadId).partNumber(7).build(),
						RequestBody.fromFile(new File("smallfile7.csv")));
		eTagToPartNumberMapping.put(uploadPartResponse.eTag(), 7);

		uploadPartResponse = s3ClientWithTransferAccelerationEnabled
				.uploadPart(
						UploadPartRequest.builder().bucket(transferAcceleratedBucket).key("bigfile.csv")
								.uploadId(uploadId).partNumber(8).build(),
						RequestBody.fromFile(new File("smallfile8.csv")));
		eTagToPartNumberMapping.put(uploadPartResponse.eTag(), 8);

		uploadPartResponse = s3ClientWithTransferAccelerationEnabled
				.uploadPart(
						UploadPartRequest.builder().bucket(transferAcceleratedBucket).key("bigfile.csv")
								.uploadId(uploadId).partNumber(9).build(),
						RequestBody.fromFile(new File("smallfile9.csv")));
		eTagToPartNumberMapping.put(uploadPartResponse.eTag(), 9);

		uploadPartResponse = s3ClientWithTransferAccelerationEnabled
				.uploadPart(
						UploadPartRequest.builder().bucket(transferAcceleratedBucket).key("bigfile.csv")
								.uploadId(uploadId).partNumber(10).build(),
						RequestBody.fromFile(new File("smallfile10.csv")));

		eTagToPartNumberMapping.put(uploadPartResponse.eTag(), 10);
		List<CompletedPart> completedParts = eTagToPartNumberMapping.entrySet().stream()
				.map(entry -> CompletedPart.builder().eTag(entry.getKey()).partNumber(entry.getValue()).build())
				.toList();
		s3ClientWithTransferAccelerationEnabled.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
				.bucket(transferAcceleratedBucket).key("bigfile.csv").uploadId(uploadId)
				.multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build()).build());
		long endTime = System.currentTimeMillis();
		System.out.println(
				"Time taken to put a 2 GB object from india to north virginia with transfer acceleration and with multipart upload took "
						+ (endTime - startTime) / 1000 + " seconds.");
	}

	private static void uploadObjectUsingMultipartUploadWithoutTransferAcceleration() throws InterruptedException {
		Map<String, Integer> eTagToPartNumberMapping = new ConcurrentHashMap<>();
		long startTime = System.currentTimeMillis();
		CreateMultipartUploadResponse createMultipartUploadResponse = s3Client.createMultipartUpload(
				CreateMultipartUploadRequest.builder().bucket(plainBucket).key("bigfile.csv").build());
		String uploadId = createMultipartUploadResponse.uploadId();

		UploadPartResponse uploadPartResponse = s3Client.uploadPart(UploadPartRequest.builder().bucket(plainBucket)
				.key("bigfile.csv").uploadId(uploadId).partNumber(1).build(),
				RequestBody.fromFile(new File("smallfile1.csv")));
		eTagToPartNumberMapping.put(uploadPartResponse.eTag(), 1);

		uploadPartResponse = s3Client.uploadPart(UploadPartRequest.builder().bucket(plainBucket).key("bigfile.csv")
				.uploadId(uploadId).partNumber(2).build(), RequestBody.fromFile(new File("smallfile2.csv")));
		eTagToPartNumberMapping.put(uploadPartResponse.eTag(), 2);

		uploadPartResponse = s3Client.uploadPart(UploadPartRequest.builder().bucket(plainBucket).key("bigfile.csv")
				.uploadId(uploadId).partNumber(3).build(), RequestBody.fromFile(new File("smallfile3.csv")));
		eTagToPartNumberMapping.put(uploadPartResponse.eTag(), 3);

		uploadPartResponse = s3Client.uploadPart(UploadPartRequest.builder().bucket(plainBucket).key("bigfile.csv")
				.uploadId(uploadId).partNumber(4).build(), RequestBody.fromFile(new File("smallfile4.csv")));
		eTagToPartNumberMapping.put(uploadPartResponse.eTag(), 4);

		uploadPartResponse = s3Client.uploadPart(UploadPartRequest.builder().bucket(plainBucket).key("bigfile.csv")
				.uploadId(uploadId).partNumber(5).build(), RequestBody.fromFile(new File("smallfile5.csv")));
		eTagToPartNumberMapping.put(uploadPartResponse.eTag(), 5);

		uploadPartResponse = s3Client.uploadPart(UploadPartRequest.builder().bucket(plainBucket).key("bigfile.csv")
				.uploadId(uploadId).partNumber(6).build(), RequestBody.fromFile(new File("smallfile6.csv")));
		eTagToPartNumberMapping.put(uploadPartResponse.eTag(), 6);

		uploadPartResponse = s3Client.uploadPart(UploadPartRequest.builder().bucket(plainBucket).key("bigfile.csv")
				.uploadId(uploadId).partNumber(7).build(), RequestBody.fromFile(new File("smallfile7.csv")));
		eTagToPartNumberMapping.put(uploadPartResponse.eTag(), 7);

		uploadPartResponse = s3Client.uploadPart(UploadPartRequest.builder().bucket(plainBucket).key("bigfile.csv")
				.uploadId(uploadId).partNumber(8).build(), RequestBody.fromFile(new File("smallfile8.csv")));
		eTagToPartNumberMapping.put(uploadPartResponse.eTag(), 8);

		uploadPartResponse = s3Client.uploadPart(UploadPartRequest.builder().bucket(plainBucket).key("bigfile.csv")
				.uploadId(uploadId).partNumber(9).build(), RequestBody.fromFile(new File("smallfile9.csv")));
		eTagToPartNumberMapping.put(uploadPartResponse.eTag(), 9);

		uploadPartResponse = s3Client.uploadPart(UploadPartRequest.builder().bucket(plainBucket).key("bigfile.csv")
				.uploadId(uploadId).partNumber(10).build(), RequestBody.fromFile(new File("smallfile10.csv")));
		eTagToPartNumberMapping.put(uploadPartResponse.eTag(), 10);

		List<CompletedPart> completedParts = eTagToPartNumberMapping.entrySet().stream()
				.map(entry -> CompletedPart.builder().eTag(entry.getKey()).partNumber(entry.getValue()).build())
				.toList();
		s3Client.completeMultipartUpload(
				CompleteMultipartUploadRequest.builder().bucket(plainBucket).key("bigfile.csv").uploadId(uploadId)
						.multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build()).build());
		long endTime = System.currentTimeMillis();
		System.out.println(
				"Time taken to put a 2 GB object from india to north virginia without transfer acceleration and with multipart upload took "
						+ (endTime - startTime) / 1000 + " seconds.");
	}

	private static void createPlainAndTransferAcceleratedBucket() {
		s3Client.createBucket(CreateBucketRequest.builder().bucket(plainBucket).build());
		s3Client.createBucket(CreateBucketRequest.builder().bucket(transferAcceleratedBucket).build());
		s3Client.putBucketAccelerateConfiguration(
				PutBucketAccelerateConfigurationRequest.builder().bucket(transferAcceleratedBucket)
						.accelerateConfiguration(
								AccelerateConfiguration.builder().status(BucketAccelerateStatus.ENABLED).build())
						.build());
	}

	private static void generateFilesWithFakeData()
			throws IOException, StreamWriteException, DatabindException, InterruptedException {
		Faker faker = new Faker();
		CsvMapper csvMapper = new CsvMapper();
		CsvSchema csvSchema = csvMapper.schemaFor(Person.class);
		ObjectWriter personObjectWriter = csvMapper.writer(csvSchema);
		ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

		List<Person> somePeople = new ArrayList<Person>(10000000);
		for (int i = 0; i < 2000000; i++) {
			somePeople.add(new Person(faker.name().firstName(), faker.name().lastName(), faker.company().profession(),
					faker.chuckNorris().fact()));
		}

		executorService.execute(getRunnableToGenerateSmallFile(personObjectWriter, somePeople, 1));
		executorService.execute(getRunnableToGenerateSmallFile(personObjectWriter, somePeople, 2));
		executorService.execute(getRunnableToGenerateSmallFile(personObjectWriter, somePeople, 3));
		executorService.execute(getRunnableToGenerateSmallFile(personObjectWriter, somePeople, 4));
		executorService.execute(getRunnableToGenerateSmallFile(personObjectWriter, somePeople, 5));
		executorService.execute(getRunnableToGenerateSmallFile(personObjectWriter, somePeople, 6));
		executorService.execute(getRunnableToGenerateSmallFile(personObjectWriter, somePeople, 7));
		executorService.execute(getRunnableToGenerateSmallFile(personObjectWriter, somePeople, 8));
		executorService.execute(getRunnableToGenerateSmallFile(personObjectWriter, somePeople, 9));
		executorService.execute(getRunnableToGenerateSmallFile(personObjectWriter, somePeople, 10));

		executorService.shutdown();
		while (!executorService.awaitTermination(1, TimeUnit.MINUTES))
			;

	}

	private static Runnable getRunnableToGenerateSmallFile(ObjectWriter personObjectWriter, List<Person> somePeople,
			int i) {
		return () -> {
			try {
				File file = new File("smallfile" + i + ".csv");
				personObjectWriter.writeValue(file, somePeople);
			} catch (IOException e) {
				e.printStackTrace();
			}
		};
	}

}
