import { createHmac } from "node:crypto";
import {
  ChallengeNameType,
  CognitoIdentityProviderClient,
  CognitoIdentityProviderClientConfig as AwsSdkCognitoClientConfig,
  InitiateAuthCommand,
  InitiateAuthCommandOutput,
  RespondToAuthChallengeCommand,
  RespondToAuthChallengeCommandOutput,
} from "@aws-sdk/client-cognito-identity-provider";
import { CognitoJwtVerifier } from "aws-jwt-verify";
import {
  CognitoAccessTokenPayload,
  CognitoIdTokenPayload,
} from "aws-jwt-verify/jwt-model";
import { ClientLogger, createConsoleLogger } from "../logger";

const defaultLogger = createConsoleLogger("aws-cognito-client");

export interface AwsCognitoClientConfig {
  region: string;
  userPoolId: string;
  appClientId: string;
  appClientSecret?: string;
  options?: Omit<AwsSdkCognitoClientConfig, "region" | "credentials">;
  logger?: ClientLogger;
}

export interface AwsCognitoChallengeConfig {
  challengeName: ChallengeNameType;
  session: string;
  username: string;
  responses: Record<string, string>;
}

export interface AwsCognitoClient {
  client: CognitoIdentityProviderClient;
  signIn(username: string, password: string): Promise<InitiateAuthCommandOutput>;
  refreshTokens(
    refreshToken: string,
    username?: string,
  ): Promise<InitiateAuthCommandOutput>;
  respondToChallenge(
    config: AwsCognitoChallengeConfig,
  ): Promise<RespondToAuthChallengeCommandOutput>;
  verifyAccessToken(token: string): Promise<CognitoAccessTokenPayload>;
  verifyIdToken(token: string): Promise<CognitoIdTokenPayload>;
  close(): void;
}

function requireText(value: string, field: string): void {
  if (!value.trim()) {
    throw new Error(`${field} is required`);
  }
}

export function createAwsCognitoSecretHash(
  username: string,
  appClientId: string,
  appClientSecret: string,
): string {
  requireText(username, "Cognito username");
  requireText(appClientId, "Cognito app client ID");
  requireText(appClientSecret, "Cognito app client secret");

  return createHmac("sha256", appClientSecret)
    .update(`${username}${appClientId}`)
    .digest("base64");
}

export function createAwsCognitoClient(
  config: AwsCognitoClientConfig,
): AwsCognitoClient {
  requireText(config.region, "AWS region");
  requireText(config.userPoolId, "Cognito user pool ID");
  requireText(config.appClientId, "Cognito app client ID");

  const logger = config.logger ?? defaultLogger;
  const context = {
    region: config.region,
    userPoolId: config.userPoolId,
    appClientId: config.appClientId,
  };
  const client = new CognitoIdentityProviderClient({
    ...config.options,
    region: config.region,
  });
  const accessTokenVerifier = CognitoJwtVerifier.create({
    userPoolId: config.userPoolId,
    clientId: config.appClientId,
    tokenUse: "access",
  });
  const idTokenVerifier = CognitoJwtVerifier.create({
    userPoolId: config.userPoolId,
    clientId: config.appClientId,
    tokenUse: "id",
  });

  function secretHash(username: string): string | undefined {
    return config.appClientSecret
      ? createAwsCognitoSecretHash(
          username,
          config.appClientId,
          config.appClientSecret,
        )
      : undefined;
  }

  logger.info("Cognito client created", context);

  return {
    client,
    async signIn(username, password) {
      requireText(username, "Cognito username");
      requireText(password, "Cognito password");

      const output = await client.send(
        new InitiateAuthCommand({
          AuthFlow: "USER_PASSWORD_AUTH",
          ClientId: config.appClientId,
          AuthParameters: {
            USERNAME: username,
            PASSWORD: password,
            ...(config.appClientSecret
              ? { SECRET_HASH: secretHash(username) }
              : {}),
          },
        }),
      );

      logger.info("Cognito sign-in request completed", {
        ...context,
        challengeName: output.ChallengeName,
        authenticated: Boolean(output.AuthenticationResult),
      });
      return output;
    },
    async refreshTokens(refreshToken, username) {
      requireText(refreshToken, "Cognito refresh token");

      if (config.appClientSecret && !username?.trim()) {
        throw new Error(
          "Cognito username is required to refresh tokens when the app client has a secret",
        );
      }

      const output = await client.send(
        new InitiateAuthCommand({
          AuthFlow: "REFRESH_TOKEN_AUTH",
          ClientId: config.appClientId,
          AuthParameters: {
            REFRESH_TOKEN: refreshToken,
            ...(config.appClientSecret && username
              ? { SECRET_HASH: secretHash(username) }
              : {}),
          },
        }),
      );

      logger.info("Cognito token refresh completed", context);
      return output;
    },
    async respondToChallenge(challengeConfig) {
      requireText(challengeConfig.session, "Cognito challenge session");
      requireText(challengeConfig.username, "Cognito username");

      const output = await client.send(
        new RespondToAuthChallengeCommand({
          ChallengeName: challengeConfig.challengeName,
          ClientId: config.appClientId,
          Session: challengeConfig.session,
          ChallengeResponses: {
            ...challengeConfig.responses,
            USERNAME: challengeConfig.username,
            ...(config.appClientSecret
              ? { SECRET_HASH: secretHash(challengeConfig.username) }
              : {}),
          },
        }),
      );

      logger.info("Cognito challenge response completed", {
        ...context,
        challengeName: output.ChallengeName,
        authenticated: Boolean(output.AuthenticationResult),
      });
      return output;
    },
    async verifyAccessToken(token) {
      requireText(token, "Cognito access token");
      const payload = await accessTokenVerifier.verify(token);
      logger.debug("Cognito access token verified", context);
      return payload;
    },
    async verifyIdToken(token) {
      requireText(token, "Cognito ID token");
      const payload = await idTokenVerifier.verify(token);
      logger.debug("Cognito ID token verified", context);
      return payload;
    },
    close() {
      client.destroy();
      logger.info("Cognito client closed", context);
    },
  };
}
