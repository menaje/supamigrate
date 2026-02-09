# supamigrate

Supabase Cloud에서 Self-hosted Supabase로 데이터베이스를 마이그레이션하는 도구입니다.

## Features

- **Schema Migration** - 테이블, 시퀀스, 인덱스, 제약조건, Enum 타입, Views
- **Functions & Triggers** - 저장 함수 및 트리거 (RPC 포함)
- **Data Migration** - FK 의존성 순서로 배치 처리
- **RLS Policies** - Row Level Security 정책
- **GRANTs** - 역할 권한 (anon, authenticated, service_role)
- **Storage** - 버킷 및 파일 마이그레이션
- **SQL Export/Import** - 스키마를 SQL로 내보내고 적용
- **Verification** - 마이그레이션 후 데이터 검증

## Installation

```bash
npm install
```

## Configuration

`.env` 파일을 생성하고 다음 환경변수를 설정합니다:

```bash
# ===========================================
# Source (Supabase Cloud)
# ===========================================
SOURCE_SUPABASE_URL=https://[project-ref].supabase.co
SOURCE_SUPABASE_SERVICE_KEY=eyJ...your-service-role-key
SOURCE_DB_PASSWORD=your-database-password

# ===========================================
# Target (Self-hosted Supabase)
# ===========================================
TARGET_SUPABASE_URL=http://localhost:8000
TARGET_SUPABASE_SERVICE_KEY=eyJ...your-service-role-key
TARGET_DB_PASSWORD=your-database-password

# ===========================================
# Migration Options
# ===========================================
MIGRATE_SCHEMAS=public
BATCH_SIZE=1000
```

### 자동 연결 (권장)

DB 연결 정보가 **자동으로 생성**됩니다:
- Project Ref는 Service Key JWT에서 자동 추출
- Pooler Region은 자동 탐지 (Seoul, Virginia, Frankfurt 등)
- Connection String 자동 구성

### 수동 설정 (선택)

자동 탐지가 실패하거나 직접 지정하고 싶을 때:

```bash
# Advanced (Optional)
SOURCE_DB_URL=postgresql://postgres.[ref]:[password]@[region].pooler.supabase.com:6543/postgres
TARGET_DB_URL=postgresql://postgres:[password]@localhost:54322/postgres
```

### Supabase Cloud 연결 정보 확인

1. [Supabase Dashboard](https://supabase.com/dashboard) 접속
2. Settings → API → `service_role` 키 복사
3. Settings → Database → Database Password 복사

## Usage

### 전체 마이그레이션

```bash
npm run migrate
```

### 특정 단계만 실행

```bash
# 스키마만
npm run migrate -- --schema

# 스키마 + 데이터
npm run migrate -- --schema --data

# RLS 정책만
npm run migrate -- --rls

# 권한만
npm run migrate -- --grants

# Storage만 (버킷 + 파일)
npm run migrate -- --storage

# 데이터 검증만
npm run migrate -- --verify
```

### SQL 파일로 내보내기

데이터를 제외한 스키마 구조를 SQL로 내보냅니다:

```bash
npm run migrate -- --export-sql
```

생성 파일:
- `migration-schema.sql` - 테이블, 인덱스, 제약조건, 뷰, Enum, Sequences
- `migration-functions.sql` - Functions & Triggers
- `migration-rls.sql` - RLS 정책
- `migration-grants.sql` - 권한 설정
- `migration-complete.sql` - 전체 통합

### SQL 파일 적용

내보낸 SQL을 타겟 DB에 적용합니다:

```bash
# 기본 파일 (migration-complete.sql) 적용
npm run migrate -- --apply-sql

# 특정 파일 적용
npm run migrate -- --apply-sql migration-schema.sql
```

### 도움말

```bash
npm run migrate -- --help
```

## Migration Order

마이그레이션은 다음 순서로 실행됩니다:

1. **Schema** - Extensions, Enums, Sequences, Tables, Indexes, Constraints, Views
2. **Functions** - Stored procedures and functions (RPC)
3. **Triggers** - Database triggers
4. **Data** - Table data (sorted by FK dependencies)
5. **RLS** - Row Level Security policies
6. **GRANTs** - Role permissions
7. **Storage** - Buckets and files
8. **Verify** - Row count verification

## Options

| Option | Description |
|--------|-------------|
| `--schema` | 스키마 마이그레이션 (테이블, 인덱스, 뷰 등) |
| `--functions` | Functions 마이그레이션 |
| `--triggers` | Triggers 마이그레이션 |
| `--data` | 데이터 마이그레이션 |
| `--rls` | RLS 정책 마이그레이션 |
| `--grants` | 권한 마이그레이션 |
| `--storage` | Storage 마이그레이션 (버킷 + 파일) |
| `--verify` | 데이터 검증 |
| `--all` | 전체 마이그레이션 (기본값) |
| `--export-sql` | SQL 파일로 내보내기 |
| `--apply-sql [file]` | SQL 파일을 타겟 DB에 적용 |
| `--dry-run` | 실행 없이 계획만 표시 |

## Project Structure

```
supamigrate/
├── package.json
├── tsconfig.json
├── .env.example
├── README.md
└── src/
    ├── index.ts            # CLI 엔트리포인트
    ├── config.ts           # DB 연결 및 설정
    ├── migrate-schema.ts   # 스키마 마이그레이션 (Views, GRANTs 포함)
    ├── migrate-rls.ts      # RLS 정책 마이그레이션
    ├── migrate-functions.ts # Functions/Triggers
    ├── migrate-data.ts     # 데이터 마이그레이션
    └── migrate-storage.ts  # Storage 마이그레이션
```

## Limitations

- **auth/storage 스키마**: Supabase가 자동 관리하므로 제외됨
- **Auth Users**: `auth.users` 테이블은 비밀번호 해시 포함으로 주의 필요
- **Extensions**: 일부 확장은 Self-hosted에서 수동 설치 필요
- **Realtime**: Realtime 설정은 별도 구성 필요

## Troubleshooting

### Connection refused

```
Error: connect ECONNREFUSED
```

- Self-hosted Supabase가 실행 중인지 확인
- 포트 번호 확인 (기본 54322)
- Pooler 사용 시 포트 6543

### Permission denied

```
Error: permission denied for schema
```

- `postgres` 사용자로 연결했는지 확인
- Supabase Cloud에서 Database Password 사용
- `auth`/`storage` 스키마는 Supabase가 관리하므로 `MIGRATE_SCHEMAS=public` 사용

### SSL required

```
Error: SSL connection required
```

Supabase Cloud는 SSL이 필수입니다. Connection String 사용 시 자동 처리됩니다.

### Storage migration requires API keys

```
⚠️ SOURCE_SUPABASE_URL and SOURCE_SUPABASE_SERVICE_KEY required
```

Storage 마이그레이션은 Supabase JS Client를 사용하므로 URL과 Service Key가 필요합니다.

## License

MIT
