# Xomify Backend

AWS Lambda-based backend for the Xomify Spotify analytics application.

## Features

### 🔐 Authentication

- JWT-based API authorization
- Spotify OAuth token refresh handling
- Secure credential management via AWS SSM

### 📊 Monthly Wrapped Cron Job

- Runs monthly to capture listening data
- Saves top tracks, artists, and genres for 3 time ranges
- Creates personalized Spotify playlists automatically
- Stores unlimited history in DynamoDB

### 📅 Release Radar Cron Job

- Runs weekly to find new releases
- Scans all followed artists for recent releases
- Creates/updates a playlist with new tracks
- Handles albums, singles, and compilations

### 👤 User Management

- User enrollment for Wrapped and Release Radar features
- Refresh token storage and management
- User preferences and settings

## Architecture

```
lambdas/
├── authorizer/            # JWT token validation
│   └── handler.py
├── common/                # Shared utilities
│   ├── aiohttp_helper.py  # Async HTTP with rate limiting
│   ├── artist_list.py     # Artist data handling
│   ├── constants.py       # Configuration constants
│   ├── dynamo_helpers.py  # DynamoDB operations
│   ├── errors.py          # Custom exceptions
│   ├── logger.py          # Logging configuration
│   ├── playlist.py        # Spotify playlist operations
│   ├── spotify.py         # Spotify API client
│   ├── ssm_helpers.py     # AWS SSM parameter access
│   ├── track_list.py      # Track data handling
│   ├── utility_helpers.py # Response helpers
│   └── wrapped_helper.py  # User queries
├── release_radar/         # Weekly release radar
│   ├── handler.py
│   ├── weekly_release_radar.py
│   └── weekly_release_radar_aiohttp.py
├── update_user_table/     # User management
│   └── handler.py
└── wrapped/               # Monthly wrapped
    ├── handler.py
    ├── monthly_wrapped.py
    ├── monthly_wrapped_aiohttp.py
    └── wrapped_data.py
```

## DynamoDB Tables

### xomify-user (Main User Table)

| Attribute          | Type        | Description                   |
| ------------------ | ----------- | ----------------------------- |
| email              | String (PK) | User's email                  |
| userId             | String      | Spotify user ID               |
| refreshToken       | String      | Spotify refresh token         |
| active             | Boolean     | Account active status         |
| activeWrapped      | Boolean     | Enrolled in Wrapped           |
| activeReleaseRadar | Boolean     | Enrolled in Release Radar     |
| releaseRadarId     | String      | Playlist ID for release radar |
| updatedAt          | String      | Last update timestamp         |

### xomify-wrapped-history (Wrapped History)

| Attribute    | Type        | Description                |
| ------------ | ----------- | -------------------------- |
| email        | String (PK) | User's email               |
| monthKey     | String (SK) | Month identifier "YYYY-MM" |
| topSongIds   | Map         | Track IDs by time range    |
| topArtistIds | Map         | Artist IDs by time range   |
| topGenres    | Map         | Genre counts by time range |
| createdAt    | String      | Creation timestamp         |

## API Endpoints

### Wrapped Service (`/wrapped`)

**GET** `/wrapped/data?email={email}`
Returns user's enrollment status and all wrapped history.

**GET** `/wrapped/month?email={email}&monthKey={YYYY-MM}`
Returns single month's wrapped data.

**GET** `/wrapped/year?email={email}&year={YYYY}`
Returns all wrapped data for a year.

**POST** `/wrapped/data`
Opt user in/out of monthly wrapped.

### User Service (`/user`)

**GET** `/user/user-table?email={email}`
Returns user data including enrollment status.

**POST** `/user/user-table`
Update user enrollments or refresh token.

## Environment Setup

### AWS SSM Parameters Required

```
/xomify/aws/ACCESS_KEY
/xomify/aws/SECRET_KEY
/xomify/spotify/CLIENT_ID
/xomify/spotify/CLIENT_SECRET
/xomify/api/API_SECRET_KEY
```

### Constants Configuration

```python
# constants.py
PRODUCT = 'xomify'
AWS_DEFAULT_REGION = 'us-east-1'
USERS_TABLE_NAME = 'xomify-user'
WRAPPED_HISTORY_TABLE_NAME = 'xomify-wrapped-history'
DYNAMODB_KMS_ALIAS = 'alias/xomify-kms'
```

## Cron Job Schedules

- **Monthly Wrapped**: 1st of each month at 00:00 UTC
- **Release Radar**: Every Sunday at 00:00 UTC

Configure via AWS EventBridge rules with source `aws.events`.

## Deployment

```bash
# Package Lambda
zip -r lambda.zip lambdas/

# Deploy via Terraform/CloudFormation
# Or upload directly to AWS Lambda
```

## Rate Limiting

The backend includes built-in rate limit handling for Spotify API:

- Global rate limit tracking across concurrent requests
- Automatic retry with exponential backoff
- Respects Spotify's `Retry-After` headers

## Error Handling

Custom exception classes for each service:

- `LambdaAuthorizerError`
- `WrappednError`
- `ReleaseRadarError`
- `UpdateUserTableError`
- `DynamodbError`

All errors return structured JSON responses with status codes.
