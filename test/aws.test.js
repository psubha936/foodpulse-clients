const assert = require("node:assert/strict");
const { createHmac } = require("node:crypto");
const test = require("node:test");
const {
  createAwsCognitoClient,
  createAwsCognitoSecretHash,
  createAwsS3Client,
} = require("../dist");

test("Cognito secret hash uses username and app client ID", () => {
  const username = "customer-integration-1";
  const clientId = "foodpulse-test-client";
  const clientSecret = "not-a-real-secret";
  const expected = createHmac("sha256", clientSecret)
    .update(`${username}${clientId}`)
    .digest("base64");

  assert.equal(
    createAwsCognitoSecretHash(username, clientId, clientSecret),
    expected,
  );
});

test("Cognito client validates required configuration without an AWS call", () => {
  assert.throws(
    () =>
      createAwsCognitoClient({
        region: "",
        userPoolId: "ap-south-1_example",
        appClientId: "foodpulse-test-client",
      }),
    /AWS region is required/,
  );

  const cognito = createAwsCognitoClient({
    region: "ap-south-1",
    userPoolId: "ap-south-1_example",
    appClientId: "foodpulse-test-client",
  });

  assert.ok(cognito.client);
  cognito.close();
});

test("S3 client creates signed URLs from the SDK environment provider", async () => {
  const previousAccessKeyId = process.env.AWS_ACCESS_KEY_ID;
  const previousSecretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;

  process.env.AWS_ACCESS_KEY_ID = "AKIAFAKEINTEGRATION";
  process.env.AWS_SECRET_ACCESS_KEY = "fake-integration-secret";

  const s3 = createAwsS3Client({
    region: "ap-south-1",
    bucket: "foodpulse-integration-example",
  });

  try {
    const uploadUrl = await s3.createUploadUrl({
      key: "test/image.jpg",
      contentType: "image/jpeg",
      expiresInSeconds: 60,
    });
    const downloadUrl = await s3.createDownloadUrl({
      key: "test/image.jpg",
      expiresInSeconds: 60,
    });

    assert.match(uploadUrl, /^https:\/\//);
    assert.match(uploadUrl, /X-Amz-Signature=/);
    assert.match(downloadUrl, /X-Amz-Signature=/);
  } finally {
    s3.close();

    if (previousAccessKeyId === undefined) {
      delete process.env.AWS_ACCESS_KEY_ID;
    } else {
      process.env.AWS_ACCESS_KEY_ID = previousAccessKeyId;
    }

    if (previousSecretAccessKey === undefined) {
      delete process.env.AWS_SECRET_ACCESS_KEY;
    } else {
      process.env.AWS_SECRET_ACCESS_KEY = previousSecretAccessKey;
    }
  }
});

test("S3 client rejects unsafe URL expiry values", async () => {
  const s3 = createAwsS3Client({
    region: "ap-south-1",
    bucket: "foodpulse-integration-example",
  });

  try {
    await assert.rejects(
      s3.createDownloadUrl({
        key: "test/image.jpg",
        expiresInSeconds: 0,
      }),
      /S3 URL expiry/,
    );
  } finally {
    s3.close();
  }
});
