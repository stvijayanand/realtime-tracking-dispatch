@REM Maven Wrapper for Windows
@REM Generated for the Dispatch Service — do not edit manually.

@IF "%__MVNW_ARG0_NAME__%"=="" (SET "MVN_CMD=mvn") ELSE (SET "MVN_CMD=%__MVNW_ARG0_NAME__%")
@SET MAVEN_WRAPPER_JAR="%~dp0.mvn\wrapper\maven-wrapper.jar"
@SET MAVEN_WRAPPER_PROPERTIES="%~dp0.mvn\wrapper\maven-wrapper.properties"

@IF NOT EXIST %MAVEN_WRAPPER_JAR% (
    @FOR /F "usebackq tokens=2 delims==" %%A IN (`findstr /i "wrapperUrl" %MAVEN_WRAPPER_PROPERTIES%`) DO (
        @SET WRAPPER_URL=%%A
    )
    @ECHO Downloading Maven Wrapper from %WRAPPER_URL%
    @powershell -Command "Invoke-WebRequest -Uri '%WRAPPER_URL%' -OutFile %MAVEN_WRAPPER_JAR%"
)

@"%JAVA_HOME%\bin\java.exe" ^
    -classpath %MAVEN_WRAPPER_JAR% ^
    "-Dmaven.multiModuleProjectDirectory=%~dp0" ^
    org.apache.maven.wrapper.MavenWrapperMain %*
