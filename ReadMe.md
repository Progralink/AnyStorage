# AnyStorage Java Library - unified API for multiple storage types


## How To Use It
Compatible with Java 8 and higher. Add Maven dependency to your project:
```
<dependency>
    <groupId>com.progralink.anystorage</groupId>
    <artifactId>anystorage-all</artifactId>
    <version>1.0.0</version>
</dependency>
```
Instead of `anystorage-all` you can pick one specific connector from the list below:


### Supported Storages
- `anystorage-memory`: Java Heap memory
- `anystorage-filesystem`: local filesystem *(whatever Java 8+ supports on host OS)*
- `anystorage-smb`: SMB/CIFS (NAS), uses `org.codelibs:jcifs`
- `anystorage-aws-s3`: AWS S3 (or compatible), uses `software.amazon.awssdk:s3`
- `anystorage-sql`: SQL database *(mimics filesystem in a table)*


## Dependencies
- for all: **JInOut** | https://github.com/Progralink/JInOut (licence: LGPL)
- for SMB/CIFS: **jcifs** | https://github.com/codelibs/jcifs (licence: LGPL) + its dependencies 
- for AWS S3: **AWS Java SDK: S3** | https://aws.amazon.com/sdk-for-java/ (license: Apache 2.0) + its dependencies 

## Examples

### Connect
Use `StorageConnectors` class to automatically find proper connector that handles specific URL: 
```
String url = "smb://user:password@nas/sharename/dir";
Options options = Options.DEFAULTS; //optionally set additional options for that connector
StorageSession session = new StorageConnectors().provide("My NAS", url, options).orElse(null);
```

### Write and Read
```
StorageResource file = session.getResource().child("test.txt");
file.write("Hello World".getBytes(StandardCharsets.UTF_8));
byte[] data = file.readFully();
```

### Immutable file
Write-Once Read-Many (WORM) mode that prevents overwriting:
```
file.write("Hello World".getBytes(StandardCharsets.UTF_8), WriteOption.CREATE_NEW, WriteOption.ATOMIC);
InputStream stream = file.openRead(ReadOption.OLDEST_VERSION);
``` 
Be aware that `WriteOption.CREATE_NEW` option tries to prevent overwriting, so it should throw `AlreadyExistsException` on second and further attempt. `WriteOption.ATOMIC` tries to assure that there will be no intermediate state. `ReadOption.OLDEST_VERSION` ensures that the oldest version will be read (in storages that supports versioning) so in case of accidental overwrite or some intermediate state always oldest one will be fetched (required in AWS S3 which doesn't natively support `CREATE_NEW` mode).  

### List Children
Use stream `resource.children()` or collect it to list like below:
```
List<StorageResource> childs = session.getResource().children().collect(Collectors.toList());
```


## Main goals and tricks

### (Almost) Atomic Write
One important goal for this library was to provide ability to put/overwrite new content without the risk that interrupted transfer could leave improper file content. AnyStorage connectors are trying to make writes as atomical as possible when using `WriteOption.ATOMIC`. How it is handled differs between connectors. While AWS S3 provides atomic writes out-of-the-box, for local filesystem AnyStorage FileSystem connector is trying to use temporary files, file locks and atomic file moves. Be aware that atomic write may be not supported with `openWrite()` (that starts `OutputStream`) but only with `write()`.    

### Immutable File Content
Another important goal for this library was to provide easy ability to create files in write-once read-many ("WORM") mode.
In most filesystems file can be created with `CREATE_NEW` flag which prevents file overwrite, but for many cloud services like AWS S3 this can be only achieved in a complex way by proper version handling and using a mix of `WriteOption.CREATE_NEW` while writing and `ReadOption.OLDEST_VERSION` while reading to be sure that the first version has been fetched. It might be worth to mix it with `WriteOption.ATOMIC`.

### Cloud Support
There's an initial AWS S3 support. Be aware that it may require additional options while writing streams, for example:
`WriteOption.ofContentLength()` and `WriteOption.ofChecksumSHA256()`

### Multiple storages
`MultiStorageSession` allows to handle multiple storage sessions using the same high-level API. Write to many storages at once to store backups live.

### Automatic directories creation
Some storages, like S3, doesn't use directories. Because of that, AnyStorage when writing the file will automatically make directories through the whole specified path for storages that support (thus require) directories. 

### Enhanced Access Security
This library tries to prevent getting out of the context (chosen "root" directory for the application storage) when using `getParent()` or `resolve(path)` methods.


## Storage Types notes

### AWS S3

#### Immutable files / versioning
AWS S3 requires the bucket to be versioned for proper overwrite prevention support.

#### Storage Class
S3 Storage Class can be provided as a dedicated write option:   
```
S3WriteOption.ofStorageClass(StorageClass.GLACIER_IR)
```
or as a default S3 Storage Class associated to the session:
```
((S3StorageSession) session).setDefaultStorageClass(StorageClass.GLACIER_IR)
```


### SQL
Creates table `storage` to keep filesystem structure and data in SQL BLOBs. Requires JDBC driver of chosen type to be present in the Classpath. Tested only with H2 https://h2database.com
```
StorageSession session = new StorageConnectors().provide("My Storage DB", "jdbc:h2:~/storage-db");
```


## License
Licensed under GNU LESSER GENERAL PUBLIC LICENSE (LGPL) 3.0: https://www.gnu.org/licenses/lgpl-3.0-standalone.html

If you need another licensing type, please contact us: https://www.progralink.com/


## Roadmap
- SFTP
- FTP
- Azure


## Alternatives
There are other fantastic libraries that are similar, but may not handle some specific cases (like overwrite prevention) or focusing on different storage types:
- Apache VFS: https://commons.apache.org/proper/commons-vfs/
- Apache JClouds BlobStore https://jclouds.apache.org/


## Need More?
If you need support or another licensing type, please contact us: https://www.progralink.com/contact/
