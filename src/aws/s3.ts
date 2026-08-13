import {
  DeleteObjectCommand,
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
  S3ClientConfig as AwsSdkS3ClientConfig,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { ClientLogger, createConsoleLogger } from "../logger";

const defaultLogger = createConsoleLogger("aws-s3-client");

export interface AwsS3ClientConfig {
  region: string;
  bucket: string;
  options?: Omit<AwsSdkS3ClientConfig, "region" | "credentials">;
  logger?: ClientLogger;
}

export interface AwsS3PresignedUploadConfig {
  key: string;
  contentType: string;
  expiresInSeconds?: number;
}

export interface AwsS3PresignedDownloadConfig {
  key: string;
  expiresInSeconds?: number;
}

export interface AwsS3Client {
  client: S3Client;
  bucket: string;
  createUploadUrl(config: AwsS3PresignedUploadConfig): Promise<string>;
  createDownloadUrl(config: AwsS3PresignedDownloadConfig): Promise<string>;
  deleteObject(key: string): Promise<void>;
  close(): void;
}

function requireText(value: string, field: string): void {
  if (!value.trim()) {
    throw new Error(`${field} is required`);
  }
}

function expiresIn(value = 900): number {
  if (!Number.isInteger(value) || value < 1 || value > 3600) {
    throw new Error("S3 URL expiry must be an integer from 1 to 3600 seconds");
  }

  return value;
}

export function createAwsS3Client(config: AwsS3ClientConfig): AwsS3Client {
  requireText(config.region, "AWS region");
  requireText(config.bucket, "S3 bucket");

  const logger = config.logger ?? defaultLogger;
  const client = new S3Client({
    ...config.options,
    region: config.region,
  });
  const context = {
    region: config.region,
    bucket: config.bucket,
  };

  logger.info("S3 client created", context);

  return {
    client,
    bucket: config.bucket,
    async createUploadUrl(uploadConfig) {
      requireText(uploadConfig.key, "S3 object key");
      requireText(uploadConfig.contentType, "S3 content type");

      const url = await getSignedUrl(
        client,
        new PutObjectCommand({
          Bucket: config.bucket,
          Key: uploadConfig.key,
          ContentType: uploadConfig.contentType,
        }),
        { expiresIn: expiresIn(uploadConfig.expiresInSeconds) },
      );

      logger.debug("S3 upload URL created", context);
      return url;
    },
    async createDownloadUrl(downloadConfig) {
      requireText(downloadConfig.key, "S3 object key");

      const url = await getSignedUrl(
        client,
        new GetObjectCommand({
          Bucket: config.bucket,
          Key: downloadConfig.key,
        }),
        { expiresIn: expiresIn(downloadConfig.expiresInSeconds) },
      );

      logger.debug("S3 download URL created", context);
      return url;
    },
    async deleteObject(key) {
      requireText(key, "S3 object key");

      await client.send(
        new DeleteObjectCommand({
          Bucket: config.bucket,
          Key: key,
        }),
      );

      logger.info("S3 object deleted", context);
    },
    close() {
      client.destroy();
      logger.info("S3 client closed", context);
    },
  };
}
