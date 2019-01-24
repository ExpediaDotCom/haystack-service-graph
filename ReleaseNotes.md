# Release Notes

## 2019-01-23 1.0.9
  * Make the parameter for listObjectsBatchSize in S3SnapshotStore optional, as it's only needed when calling write

## 2019-01-23 1.0.8
  * Remove Main companion object (it wasn't really needed) 
  * Allow URL to be specified as a parameter instead of being hard coded
  * More unit tests

## 2019-01-23 1.0.7 
  * Add Main companion class to Main object so that it can be instantiated by the Java JVM
  * Add this ReleaseNotes.md file
