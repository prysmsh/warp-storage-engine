-- Organizations
CREATE TABLE organizations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    active BOOLEAN DEFAULT true,
    settings JSONB DEFAULT '{}'
);

-- Add org_id and role to users
ALTER TABLE users ADD COLUMN org_id UUID REFERENCES organizations(id);
ALTER TABLE users ADD COLUMN role VARCHAR(50) DEFAULT 'member';
CREATE INDEX idx_users_org_id ON users(org_id);

-- Virtual bucket mappings per org
CREATE TABLE org_bucket_mappings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    virtual_bucket VARCHAR(255) NOT NULL,
    physical_bucket VARCHAR(255) NOT NULL,
    prefix VARCHAR(255) NOT NULL DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(org_id, virtual_bucket)
);
CREATE INDEX idx_org_bucket_org ON org_bucket_mappings(org_id);
