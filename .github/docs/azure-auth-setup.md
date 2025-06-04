# Azure Authentication Setup for GitHub Actions

## Overview
This document explains how to set up Azure authentication for GitHub Actions using OIDC (OpenID Connect) instead of service principals or connection strings.

## Authentication Methods

### Local Development
For local development, continue using connection strings:
```bash
STORAGE_AZURE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=youraccountname;AccountKey=yourkey;EndpointSuffix=core.windows.net"
STORAGE_AZURE_CONTAINER_NAME=your-container
```

### GitHub Actions (Recommended)
For CI/CD pipelines, use managed identity with OIDC:
```bash
STORAGE_AZURE_ACCOUNT_NAME=youraccountname
STORAGE_AZURE_USE_MANAGED_IDENTITY=true
STORAGE_AZURE_CONTAINER_NAME=your-container
```

## GitHub OIDC with Workload Identity Federation

### 1. Azure Setup

#### Create an Azure AD Application
```bash
# Create the application
az ad app create --display-name "cb-trade-data-service-github"

# Get the application ID (client ID)
APP_ID=$(az ad app list --display-name "cb-trade-data-service-github" --query "[0].appId" -o tsv)
echo "Application ID: $APP_ID"

# Create a service principal
az ad sp create --id $APP_ID
OBJECT_ID=$(az ad sp show --id $APP_ID --query "id" -o tsv)
echo "Object ID: $OBJECT_ID"
```

#### Configure Federated Credentials
```bash
# Option 1: Grant access to ALL branches in the repository (RECOMMENDED)
az ad app federated-credential create --id $APP_ID --parameters '{"name": "cb-trade-all-branches", "issuer": "https://token.actions.githubusercontent.com", "subject": "repo:your-org/cb-trade-data-service:ref:refs/heads/*", "audiences": ["api://AzureADTokenExchange"]}'

# Option 2: Grant access to the entire repository (all branches, tags, PRs)
az ad app federated-credential create --id $APP_ID --parameters '{"name": "cb-trade-full-repo", "issuer": "https://token.actions.githubusercontent.com", "subject": "repo:your-org/cb-trade-data-service:*", "audiences": ["api://AzureADTokenExchange"]}'

# Option 3: Grant access to pull requests from any branch
az ad app federated-credential create --id $APP_ID --parameters '{"name": "cb-trade-pull-requests", "issuer": "https://token.actions.githubusercontent.com", "subject": "repo:your-org/cb-trade-data-service:pull_request", "audiences": ["api://AzureADTokenExchange"]}'
```

**Alternative: Using JSON files (easier for complex configurations)**
```bash
# Create a JSON file for the federated credential
cat > fedcred.json << 'EOF'
{
  "name": "cb-trade-all-branches",
  "issuer": "https://token.actions.githubusercontent.com",
  "subject": "repo:your-org/cb-trade-data-service:ref:refs/heads/*",
  "audiences": ["api://AzureADTokenExchange"]
}
EOF

# Apply the federated credential
az ad app federated-credential create --id $APP_ID --parameters @fedcred.json

# Clean up
rm fedcred.json
```

**PowerShell version (for Windows users):**
```powershell
# PowerShell - WORKING Method: Use JSON file with @ prefix
@'
{
 "name": "github-pr-credential",
  "issuer": "https://token.actions.githubusercontent.com",
  "subject": "repo:cyberbuild/cb-trade-data-service:pull_request",
  "audiences": ["api://AzureADTokenExchange"]

}
'@ | Out-File -FilePath fedcred.json -Encoding utf8
az ad app federated-credential create --id $APP_ID --parameters '@fedcred.json'
Remove-Item fedcred.json

@'
{
  "name": "cb-trade-all-branches",
  "issuer": "https://token.actions.githubusercontent.com",
  "subject": "repo:cyberbuild/cb-trade-data-service:ref:refs/heads/*", 
  "audiences": ["api://AzureADTokenExchange"]
}
'@ | Out-File -FilePath fedcred.json -Encoding utf8
az ad app federated-credential create --id $APP_ID --parameters '@fedcred.json'
Remove-Item fedcred.json

# PowerShell - Alternative: Use cmd /c to bypass PowerShell parsing
cmd /c 'az ad app federated-credential create --id %APP_ID% --parameters "{\"name\": \"cb-trade-all-branches\", \"issuer\": \"https://token.actions.githubusercontent.com\", \"subject\": \"repo:your-org/cb-trade-data-service:ref:refs/heads/*\", \"audiences\": [\"api://AzureADTokenExchange\"]}"'

# PowerShell - Manual approach: Use Azure portal instead
# Go to Azure Portal > Azure Active Directory > App registrations > [Your App] > Certificates & secrets > Federated credentials
```

**Note for PowerShell users:** Due to PowerShell's complex JSON parsing rules, the file-based approach is most reliable. The Azure CLI documentation recommends using files for complex JSON in PowerShell.

## Subject Pattern Options

| Pattern | Description | Use Case |
|---------|-------------|----------|
| `repo:org/repo:*` | All contexts (branches, PRs, tags) | Maximum flexibility |
| `repo:org/repo:ref:refs/heads/*` | All branches | Branch-based workflows only |
| `repo:org/repo:ref:refs/heads/main` | Specific branch | Production deployments |
| `repo:org/repo:pull_request` | All pull requests | PR validation workflows |
| `repo:org/repo:environment:prod` | Specific environment | Environment-specific access |

#### Grant Storage Permissions
```bash
# Get storage account resource ID
$STORAGE_ACCOUNT="stprdtradecc"
$RESOURCE_GROUP="rg-prd-storage-cc"
$STORAGE_ID=$(az storage account show --name $STORAGE_ACCOUNT --resource-group $RESOURCE_GROUP --query "id" -o tsv)

# Grant Storage Blob Data Contributor role
az role assignment create --role "Storage Blob Data Contributor" --assignee $OBJECT_ID --scope $STORAGE_ID
```

### 2. GitHub Secrets Setup

Add these secrets to your GitHub repository (Settings > Secrets and variables > Actions):
- `AZURE_CLIENT_ID`: The application ID from step 1
- `AZURE_TENANT_ID`: Your Azure tenant ID
- `AZURE_SUBSCRIPTION_ID`: Your Azure subscription ID
- `STORAGE_AZURE_ACCOUNT_NAME`: Your storage account name
- `STORAGE_AZURE_CONTAINER_NAME`: Your container name

### 3. Storage Account Configuration

Ensure your storage account allows Azure AD authentication:
```bash
# Enable Azure AD authentication
az storage account update --name $STORAGE_ACCOUNT --resource-group $RESOURCE_GROUP --allow-shared-key-access false
```

## Code Usage

### Automatic Backend Creation
The application automatically detects the authentication method:

```python
from config import get_storage_backend

# This will create the appropriate backend based on environment variables
backend = get_storage_backend()
```

### Manual Backend Creation
For more control, you can create backends manually:

```python
from storage.backends.azure_blob_backend import AzureBlobBackend

# Using managed identity (GitHub Actions)
backend = AzureBlobBackend(
    account_name="youraccountname",
    container_name="yourcontainer",
    use_managed_identity=True
)

# Using connection string (local development)
backend = AzureBlobBackend(
    connection_string="your-connection-string",
    container_name="yourcontainer"
)
```

## Troubleshooting

### Common Issues

1. **Permission Denied**: Ensure the service principal has "Storage Blob Data Contributor" role
2. **Token Exchange Failed**: Check that federated credentials are configured correctly
3. **Container Not Found**: The application will create containers automatically if the identity has permissions

### Testing Authentication

Run the example script to test your configuration:
```bash
python examples/azure_auth_example.py
```

## Migration Guide

### From Connection Strings to Managed Identity

1. Set up Azure AD application and federated credentials (see above)
2. Add GitHub secrets for Azure authentication
3. Update GitHub Actions workflow to use OIDC login
4. Test with both authentication methods during transition
5. Remove connection string secrets once migration is complete

### Environment Variables

| Environment Variable | Local Dev | GitHub Actions | Required |
|---------------------|-----------|----------------|----------|
| `STORAGE_AZURE_CONNECTION_STRING` | ✅ | ❌ (fallback) | Local only |
| `STORAGE_AZURE_ACCOUNT_NAME` | ❌ | ✅ | CI/CD only |
| `STORAGE_AZURE_USE_MANAGED_IDENTITY` | ❌ | ✅ | CI/CD only |
| `STORAGE_AZURE_CONTAINER_NAME` | ✅ | ✅ | Always |

## Alternative: Managed Identity (for Azure-hosted runners)

If using Azure Container Instances or Azure VMs as GitHub runners:

```bash
# Assign managed identity to storage account
az role assignment create \
  --role "Storage Blob Data Contributor" \
  --assignee-object-id $MANAGED_IDENTITY_OBJECT_ID \
  --scope $STORAGE_ID
```

## Security Considerations

### Recommended Approach
For most development workflows, use **Option 1** (`repo:org/repo:ref:refs/heads/*`) which grants access to all branches but restricts access to only branch-based workflows.

### Most Permissive (Use with Caution)
**Option 2** (`repo:org/repo:*`) grants the broadest access, including:
- All branches
- Pull requests from forks
- Tags and releases
- Manual workflow dispatches

This is convenient but should only be used if you trust all contributors with write access to your repository.

### Fine-Grained Control
For production environments, consider using specific branch patterns:
```bash
# Production-only access (single line)
az ad app federated-credential create --id $APP_ID --parameters '{"name": "cb-trade-production", "issuer": "https://token.actions.githubusercontent.com", "subject": "repo:your-org/cb-trade-data-service:ref:refs/heads/main", "audiences": ["api://AzureADTokenExchange"]}'

# Development/staging access (single line)
az ad app federated-credential create --id $APP_ID --parameters '{"name": "cb-trade-staging", "issuer": "https://token.actions.githubusercontent.com", "subject": "repo:your-org/cb-trade-data-service:ref:refs/heads/develop", "audiences": ["api://AzureADTokenExchange"]}'
```

## Verification

After setting up the federated credentials, you can verify them:
```bash
# List all federated credentials for the app
az ad app federated-credential list --id $APP_ID --output table

# Test the configuration in a GitHub Action (see workflow example below)
```
