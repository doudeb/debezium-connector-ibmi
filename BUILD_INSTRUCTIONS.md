# Build Instructions for Debezium IBM i Connector with RRN Support

## Prerequisites
- **JDK 21 or higher is required** (The project won't build with JDK 11)
- Maven (or use the included `./mvnw` wrapper)

## Install JDK 21
You can install JDK 21 using one of these methods:

### Option 1: Using SDKMAN
```bash
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk install java 21-tem
```

### Option 2: Download from Adoptium
Download from: https://adoptium.net/temurin/releases/?version=21

### Option 3: Using apt (Debian/Ubuntu)
```bash
sudo apt update
sudo apt install openjdk-21-jdk
```

## Build the Project
Once JDK 21 is installed:

```bash
# Set JAVA_HOME to JDK 21
export JAVA_HOME=/path/to/jdk21
export PATH=$JAVA_HOME/bin:$PATH

# Verify Java version
java -version  # Should show version 21

# Build the project
./mvnw clean package -DskipTests
```

## Output
The compiled JAR will be located at:
- `debezium-connector-ibmi/target/debezium-connector-ibmi-3.3.0.Beta1.jar`

## Using the RRN Feature
To enable the new Relative Record Number (RRN) feature in your connector configuration, add:

```json
{
  "include.rrn.in.source": true
}
```

This will include the RRN field in the source metadata of all change events.

## What was implemented
1. Added `include.rrn.in.source` configuration field (default: false)
2. Modified journal entry parsing to capture RRN from position 10
3. Updated `EntryHeader` and `EntryHeaderDecoder` to store and retrieve RRN
4. Modified `SourceInfo` to include RRN field
5. Updated `As400SourceInfoStructMaker` to conditionally include RRN in schema
6. Modified `As400StreamingChangeEventSource` to set RRN when processing entries
7. Added RRN setter in `As400OffsetContext`

When enabled, the RRN will appear in the source structure of Kafka messages as a field named "rrn".