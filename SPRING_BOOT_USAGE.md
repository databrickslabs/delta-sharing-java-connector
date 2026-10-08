# Spring Boot 3.3.6 Compatibility

This Delta Sharing Java Connector is now compatible with **Spring Boot 3.3.6** applications.

## Jackson Version Compatibility

- **Spring Boot 3.3.6** uses Jackson `2.17.3`
- **This connector** now uses Jackson `2.17.3` (via dependency management)
- **Compatible** ✅ All tests pass with Jackson 2.17.3

## Usage in Spring Boot 3.3.6

### 1. Add the dependency to your Spring Boot project

```xml
<dependency>
    <groupId>com.databricks.labs</groupId>
    <artifactId>delta-sharing-java-connector</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

### 2. Example Spring Boot Configuration

```java
@Configuration
public class DeltaSharingConfig {
    
    @Bean
    public DeltaSharingJsonProvider deltaSharingProvider() {
        return new DeltaSharingJsonProvider();
    }
    
    @Bean
    public DeltaSharing deltaSharing() {
        return new DeltaSharing();
    }
}
```

### 3. Example Service

```java
@Service
public class DeltaSharingService {
    
    private final DeltaSharing deltaSharing;
    private final DeltaSharingJsonProvider provider;
    
    public DeltaSharingService(DeltaSharing deltaSharing, DeltaSharingJsonProvider provider) {
        this.deltaSharing = deltaSharing;
        this.provider = provider;
    }
    
    public void readDeltaTable(String profilePath, String tablePath) {
        try {
            // Read delta table
            deltaSharing.loadAsFiles(profilePath, tablePath, Optional.empty())
                .forEach(System.out::println);
        } catch (Exception e) {
            throw new RuntimeException("Failed to read delta table", e);
        }
    }
}
```

### 4. Example Application Properties

```properties
# Spring Boot application properties
spring.application.name=delta-sharing-app
spring.jackson.serialization.write-dates-as-timestamps=false
spring.jackson.deserialization.fail-on-unknown-properties=false
```

## Key Changes Made for Compatibility

1. **Dependency Management**: Added Jackson BOM 2.17.3 to override transitive dependencies
2. **Explicit Jackson Module**: Added jackson-module-scala_2.12:2.17.3 dependency  
3. **Provided Scope**: Jackson dependencies are marked as `provided` to use Spring Boot's versions
4. **No Breaking Changes**: Maintained compatibility with existing Delta Sharing APIs

## Verification

The compatibility has been verified by:
- ✅ Compilation with Jackson 2.17.3
- ✅ All 14 unit tests passing
- ✅ Successful package build
- ✅ Compatible Jackson module versions

This ensures that the connector works seamlessly in Spring Boot 3.3.6 applications without Jackson version conflicts.