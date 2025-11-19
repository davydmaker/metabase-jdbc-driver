# Release Process

This document describes how to create releases using the automated GitHub Actions workflow.

## Automated Release Process

The project uses GitHub Actions for automated building, testing, and releasing. There are two ways to trigger a release:

### Method 1: Git Tag (Recommended)

1. **Create and push a version tag:**
   ```bash
   git tag v1.2.0
   git push origin v1.2.0
   ```

2. **The workflow will automatically:**
   - Validate the version format
   - Run all tests across multiple platforms
   - Perform security scans
   - Build the JAR with dependencies
   - Generate checksums (SHA256 and MD5)
   - Create a GitHub release with release notes
   - Upload all artifacts

### Method 2: Manual Workflow Dispatch

1. **Go to GitHub Actions tab**
2. **Select "Build and Release" workflow**
3. **Click "Run workflow"**
4. **Enter the version** (e.g., `v1.2.0`)
5. **Click "Run workflow"**

## Version Format

Versions must follow semantic versioning:
- **Stable releases**: `1.2.0`, `2.0.0`, `1.5.3`
- **Pre-releases**: `1.2.0-alpha.1`, `2.0.0-beta.2`, `1.5.0-rc.1`

Pre-releases are automatically marked as "pre-release" on GitHub.

## What Gets Released

Each release includes:

1. **Main JAR file**: `metabase-jdbc-driver-{version}.jar`
   - Contains all dependencies
   - Ready to use in any JDBC client
   - Includes the official Metabase icon

2. **Checksums**:
   - `metabase-jdbc-driver-{version}.jar.sha256`
   - `metabase-jdbc-driver-{version}.jar.md5`

3. **Automatic release notes** with:
   - Installation instructions
   - DBeaver setup guide
   - Connection URL examples
   - Verification steps

## CI/CD Pipeline

### Continuous Integration (`.github/workflows/ci.yml`)

Runs on every push and pull request:
- **Multi-platform testing** (Ubuntu, Windows, macOS)
- **Multi-Java version testing** (Java 11, 17, 21)
- **Code quality analysis** with coverage reports
- **Build verification** with JAR integrity checks

### Release Pipeline (`.github/workflows/release.yml`)

Runs on version tags and manual triggers:
- **Environment validation**
- **Security scanning** with OWASP Dependency Check
- **Cross-platform testing**
- **JAR building and verification**
- **Checksum generation**
- **GitHub release creation**
- **Artifact publishing**

## Manual Release Steps (if needed)

If you need to create a release manually:

1. **Update version in pom.xml:**
   ```bash
   mvn versions:set -DnewVersion=1.2.0
   ```

2. **Build and test:**
   ```bash
   mvn clean test
   mvn package
   ```

3. **Verify JAR:**
   ```bash
   java -cp target/metabase-jdbc-driver-1.2.0-with-dependencies.jar \
        com.davydmaker.metabase.jdbc.MetabaseDriver
   ```

4. **Create release on GitHub** with the generated JAR

## Security

### Dependency Scanning

The pipeline includes:
- **OWASP Dependency Check** for known vulnerabilities
- **Configurable CVSS threshold** (currently 7.0)
- **Suppression file** for false positives

### Artifact Integrity

All releases include:
- **SHA256 checksums** for integrity verification
- **MD5 checksums** for compatibility
- **Signed commits** (if GPG signing is configured)

## Troubleshooting

### Common Issues

**Build fails on version validation:**
- Ensure version follows semantic versioning (X.Y.Z)
- Check that the tag starts with 'v' (e.g., v1.2.0)

**Security scan fails:**
- Review the OWASP report in workflow artifacts
- Update dependencies if needed
- Add suppressions to `owasp-suppression.xml` for false positives

**JAR verification fails:**
- Check that all required classes are present
- Verify MANIFEST.MF is properly configured
- Ensure services file exists for JDBC driver registration

### Getting Help

- **Workflow logs**: Check GitHub Actions tab for detailed logs
- **Artifacts**: Download build artifacts from failed runs
- **Issues**: Create an issue if the problem persists

## Rollback

If a release has issues:

1. **Mark release as pre-release** on GitHub
2. **Create hotfix branch** from the problematic tag
3. **Fix the issue** and create a new patch version
4. **Release the fixed version**

## Branch Protection

Recommended branch protection rules:
- **Require pull request reviews**
- **Require status checks** (CI must pass)
- **Require branches to be up to date**
- **Restrict pushes to main branch** 