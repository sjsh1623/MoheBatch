# Troubleshooting Guide

## Fixed Issues

### ✅ Package Structure Fixed
- **Issue**: Files were located in `src/main/com/mohe/spring/` instead of `src/main/kotlin/com/mohe/spring/`
- **Solution**: Moved all Kotlin source files to the correct directory structure
- **Status**: ✅ RESOLVED

### ✅ Dependencies Updated
- **Issue**: Some dependency versions might be incompatible
- **Solution**: Updated SpringDoc OpenAPI to version 2.1.0 for better compatibility
- **Status**: ✅ RESOLVED

## Potential Issues & Solutions

### 1. Gradle Wrapper Missing
**Issue**: The gradle-wrapper.jar file is missing or corrupted.

**Solution**:
```bash
# Download the correct Gradle distribution
cd /Users/andrewlim/Desktop/Developer/Mohe/MoheSpring
./gradlew wrapper --gradle-version 8.5
```

Or manually download from: https://services.gradle.org/distributions/gradle-8.5-bin.zip

### 2. Java Version Compatibility
**Issue**: Project requires Java 17 but different version is installed.

**Check Java version**:
```bash
java -version
javac -version
```

**Solution**: Install Java 17 if not available:
- macOS: `brew install openjdk@17`
- Set JAVA_HOME: `export JAVA_HOME=/opt/homebrew/opt/openjdk@17`

### 3. Dependency Resolution Issues
**Common Issues**:
- Internet connectivity for Maven Central
- Version conflicts between Spring Boot and other libraries

**Solution**:
```bash
./gradlew clean build --refresh-dependencies
```

### 4. IDE Configuration
**IntelliJ IDEA**:
1. Open project as Gradle project
2. Set Project SDK to Java 17
3. Enable annotation processing
4. Reimport Gradle project

**VS Code**:
1. Install Extension Pack for Java
2. Install Spring Boot Extension Pack
3. Configure Java runtime path

### 5. Database Issues
**Docker not available**:
- Install Docker Desktop
- Or use local PostgreSQL installation
- Update `application-local.yml` for local database

**Database connection**:
```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/mohe_db
    username: your_username
    password: your_password
```

## Build Commands

```bash
# Clean and build
./gradlew clean build

# Run with specific profile
./gradlew bootRun --args='--spring.profiles.active=local'

# Run tests
./gradlew test

# Check dependencies
./gradlew dependencies
```

## Verification Steps

1. **Project Structure**: ✅ Fixed - all files in correct locations
2. **Dependencies**: ✅ Fixed - compatible versions selected
3. **Java Version**: Check `java -version` outputs Java 17
4. **Gradle Build**: Run `./gradlew clean build`
5. **Application Start**: Run `./gradlew bootRun`
6. **Swagger UI**: Visit `http://localhost:8080/swagger-ui.html`

## Success Indicators

- ✅ Project compiles without errors
- ✅ All tests pass
- ✅ Application starts on port 8080
- ✅ Swagger UI loads successfully
- ✅ Health endpoint responds: `http://localhost:8080/health`
- ✅ Database schema initializes correctly

## Contact Support

If issues persist after following this guide, please provide:
1. Error messages (full stack trace)
2. Java version (`java -version`)
3. Operating system details
4. Gradle build output (`./gradlew build --info`)

The application structure is now correct and should compile successfully with proper Java 17 and Gradle setup.