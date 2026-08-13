# foodpulse-clients

Reusable Node.js clients for FoodPulse backend services:

- MongoDB
- Redis
- Kafka
- WebSocket
- Amazon S3
- Amazon Cognito

This package is server-side only. Do not install it in the Angular application.

## AWS credential boundary

The package never stores or reads FoodPulse environment variables. Calling
services pass region, bucket, user-pool ID and app-client configuration.

AWS SDK clients use the standard AWS credential provider chain. For local
development, configure a short-lived AWS CLI/Identity Center profile or put
credentials only in the calling service's ignored `.env`. In AWS, attach a
least-privilege IAM role to the Lambda, ECS task or EC2 instance. Never commit
access keys or place them in browser configuration.

## S3

```ts
import { createAwsS3Client } from "foodpulse-clients";

const s3 = createAwsS3Client({
  region: serviceConfig.awsRegion,
  bucket: serviceConfig.uploadBucket,
});

const uploadUrl = await s3.createUploadUrl({
  key: `restaurants/${restaurantId}/menu/${fileId}`,
  contentType: "image/jpeg",
  expiresInSeconds: 300,
});
```

Return the short-lived URL to an authorized browser. Validate the requested
content type, generated object key and caller permission in the service before
creating it. Do not let the browser choose an unrestricted bucket or key.

## Cognito

`identity-service` owns sign-in, refresh tokens, challenge handling and token
verification. It passes its validated environment configuration to the client:

```ts
import { createAwsCognitoClient } from "foodpulse-clients";

const cognito = createAwsCognitoClient({
  region: serviceConfig.awsRegion,
  userPoolId: serviceConfig.cognitoUserPoolId,
  appClientId: serviceConfig.cognitoAppClientId,
  appClientSecret: serviceConfig.cognitoAppClientSecret,
});
```

Do not log passwords, access tokens, ID tokens, refresh tokens, app-client
secrets or AWS credentials. Prefer secure HttpOnly cookies for refresh tokens
when the backend owns the browser session.

The Cognito app client must enable `ALLOW_USER_PASSWORD_AUTH` for `signIn()` and
`ALLOW_REFRESH_TOKEN_AUTH` for `refreshTokens()`. A challenge response may be
required for MFA or a new password before Cognito returns tokens.

## Tests

```bash
npm run build
npm run test:logger
npm run test:aws
```

MongoDB, Redis and Kafka integration tests require their external services.

## Cloud-provider organization

AWS implementations live under `src/aws` and use explicit `Aws...` export
names. If FoodPulse adopts another cloud, add separate adapters such as
`src/gcp/storage.ts` or `src/azure/storage.ts`. Calling services should hide the
selected adapter behind their own business-level interface so controllers and
domain code do not depend directly on AWS SDK types.
