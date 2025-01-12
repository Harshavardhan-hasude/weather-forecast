# ADR-02: API Key-based Authentication

## Context

We need a simple authentication method for external consumption.

## Decision

- Utilize API Gateway's built-in API key feature.

## Consequences

- Clients must provide `x-api-key` in headers.
- For stronger security, consider OAuth2 or Cognito in the future.
