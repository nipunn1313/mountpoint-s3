//! Manually implemented tests executing the FUSE protocol against [S3Filesystem]

use fuser::FileType;
use mountpoint_s3_client::ObjectClient;
#[cfg(all(feature = "s3_tests", not(feature = "s3express_tests")))]
use mountpoint_s3_client::PutObjectRequest;
#[cfg(feature = "s3_tests")]
use mountpoint_s3_client::S3CrtClient;
#[cfg(feature = "s3_tests")]
use mountpoint_s3_client::config::S3ClientConfig;
#[cfg(all(feature = "s3_tests", not(feature = "s3express_tests")))]
use mountpoint_s3_client::error_metadata::ClientErrorMetadata;
use mountpoint_s3_client::failure_client::{CountdownFailureConfig, countdown_failure_client};
use mountpoint_s3_client::mock_client::{MockClient, MockClientError, MockObject, Operation};
use mountpoint_s3_client::types::{ETag, GetObjectParams, PutObjectSingleParams, RestoreStatus};
#[cfg(feature = "s3_tests")]
use mountpoint_s3_fs::fs::error_metadata::MOUNTPOINT_ERROR_LOOKUP_NONEXISTENT;
#[cfg(all(feature = "s3_tests", not(feature = "s3express_tests")))]
use mountpoint_s3_fs::fs::error_metadata::{ErrorMetadata, MOUNTPOINT_ERROR_CLIENT};
use mountpoint_s3_fs::fs::{CacheConfig, FUSE_ROOT_INODE, OpenFlags, RenameFlags, TimeToLive, ToErrno};
use mountpoint_s3_fs::memory::PagedPool;
use mountpoint_s3_fs::s3::{Prefix, S3Personality};
use mountpoint_s3_fs::{S3Filesystem, S3FilesystemConfig};
use nix::unistd::{getgid, getuid};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use std::collections::HashMap;
use std::ffi::OsString;
use std::ops::Add;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use test_case::test_case;

mod common;
#[cfg(all(feature = "s3_tests", not(feature = "s3express_tests")))]
use common::creds::get_scoped_down_credentials;
#[cfg(feature = "s3_tests")]
use common::s3::{get_test_bucket_and_prefix, get_test_endpoint_config};
use common::{DirectoryReply, assert_attr, make_test_filesystem, make_test_filesystem_with_client};
#[cfg(all(feature = "s3_tests", not(feature = "s3express_tests")))]
use common::{get_crt_client_auth_config, s3::deny_single_object_access_policy};

#[test_case(""; "unprefixed")]
#[test_case("test_prefix/"; "prefixed")]
#[tokio::test]
async fn test_read_dir_root(prefix: &str) {
    let prefix = Prefix::new(prefix).expect("valid prefix");
    let (client, fs) = make_test_filesystem("test_read_dir", &prefix, Default::default());

    client.add_object(
        &format!("{prefix}file1.txt"),
        MockObject::constant(0xa1, 15, ETag::from_str("test_etag_1").unwrap()),
    );
    client.add_object(
        &format!("{prefix}file2.txt"),
        MockObject::constant(0xa2, 15, ETag::from_str("test_etag_2").unwrap()),
    );
    client.add_object(
        &format!("{prefix}file3.txt"),
        MockObject::constant(0xa3, 15, ETag::from_str("test_etag_3").unwrap()),
    );

    let uid = getuid().into();
    let gid = getgid().into();
    let dir_perm: u16 = 0o755;
    let file_perm: u16 = 0o644;

    // Listing the root directory doesn't require resolving it first, can just opendir the root inode
    let dir_handle = fs.opendir(FUSE_ROOT_INODE, 0).await.unwrap().fh;
    let mut reply = Default::default();
    fs.readdirplus(1, dir_handle, 0, &mut reply).await.unwrap();

    assert_eq!(reply.entries.len(), 2 + 3);

    // TODO `stat` on these needs to work
    assert_eq!(reply.entries[0].name, ".");
    assert_eq!(reply.entries[0].ino, FUSE_ROOT_INODE);
    assert_attr(reply.entries[0].attr, FileType::Directory, 0, uid, gid, dir_perm);
    assert_eq!(reply.entries[1].name, "..");
    assert_eq!(reply.entries[1].ino, FUSE_ROOT_INODE);
    assert_attr(reply.entries[1].attr, FileType::Directory, 0, uid, gid, dir_perm);

    let mut offset = reply.entries[0].offset.max(reply.entries[1].offset);
    for (i, reply) in reply.entries.iter().skip(2).enumerate() {
        let expected: OsString = format!("file{}.txt", i + 1).into();
        assert_eq!(reply.name, expected);
        assert_eq!(reply.attr.kind, FileType::RegularFile);
        assert!(reply.ino > 1);

        let attr = fs.getattr(reply.ino).await.unwrap();
        assert_eq!(attr.attr.ino, reply.ino);
        assert_attr(attr.attr, FileType::RegularFile, 15, uid, gid, file_perm);

        let fh = fs.open(reply.ino, OpenFlags::empty(), 0).await.unwrap().fh;
        let bytes_read = fs
            .read(reply.ino, fh, 0, 4096, 0, None)
            .await
            .expect("fs read should succeed");
        assert_eq!(&bytes_read[..], &[0xa0 + (i as u8 + 1); 15]);
        fs.release(reply.ino, fh, 0, None, true).await.unwrap();

        offset = offset.max(reply.offset);
    }

    assert!(offset > 0);

    let mut reply = Default::default();
    fs.readdir(FUSE_ROOT_INODE, dir_handle, offset, &mut reply)
        .await
        .unwrap();
    assert_eq!(reply.entries.len(), 0);

    fs.releasedir(FUSE_ROOT_INODE, dir_handle, 0).await.unwrap();
}

#[test_case(""; "unprefixed")]
#[test_case("test_prefix/"; "prefixed")]
#[tokio::test]
async fn test_read_dir_nested(prefix: &str) {
    let prefix = Prefix::new(prefix).expect("valid prefix");
    let (client, fs) = make_test_filesystem("test_read_dir_nested", &prefix, Default::default());

    client.add_object(
        &format!("{prefix}dir1/file1.txt"),
        MockObject::constant(0xa1, 15, ETag::from_str("test_etag_1").unwrap()),
    );
    client.add_object(
        &format!("{prefix}dir1/file2.txt"),
        MockObject::constant(0xa2, 15, ETag::from_str("test_etag_2").unwrap()),
    );
    client.add_object(
        &format!("{prefix}dir2/file3.txt"),
        MockObject::constant(0xa3, 15, ETag::from_str("test_etag_3").unwrap()),
    );

    let uid = getuid().into();
    let gid = getgid().into();
    let dir_perm: u16 = 0o755;
    let file_perm: u16 = 0o644;

    let entry = fs.lookup(FUSE_ROOT_INODE, "dir1".as_ref()).await.unwrap();
    assert_eq!(entry.attr.kind, FileType::Directory);
    let dir_ino = entry.attr.ino;

    let dir_handle = fs.opendir(dir_ino, 0).await.unwrap().fh;
    let mut reply = Default::default();
    fs.readdirplus(dir_ino, dir_handle, 0, &mut reply).await.unwrap();

    assert_eq!(reply.entries.len(), 2 + 2);

    assert_eq!(reply.entries[0].name, ".");
    assert_eq!(reply.entries[0].ino, dir_ino);
    assert_attr(reply.entries[0].attr, FileType::Directory, 0, uid, gid, dir_perm);
    assert_eq!(reply.entries[1].name, "..");
    assert_eq!(reply.entries[1].ino, FUSE_ROOT_INODE);
    assert_attr(reply.entries[1].attr, FileType::Directory, 0, uid, gid, dir_perm);

    let mut offset = reply.entries[0].offset.max(reply.entries[1].offset);
    for (i, reply) in reply.entries.iter().skip(2).enumerate() {
        let expected: OsString = format!("file{}.txt", i + 1).into();
        assert_eq!(reply.name, expected);
        assert_eq!(reply.attr.kind, FileType::RegularFile);
        assert!(reply.ino > 1);

        let attr = fs.getattr(reply.ino).await.unwrap();
        assert_eq!(attr.attr.ino, reply.ino);
        assert_attr(attr.attr, FileType::RegularFile, 15, uid, gid, file_perm);

        let fh = fs.open(reply.ino, OpenFlags::empty(), 0).await.unwrap().fh;
        let bytes_read = fs
            .read(reply.ino, fh, 0, 4096, 0, None)
            .await
            .expect("fs read should succeed");
        assert_eq!(&bytes_read[..], &[0xa0 + (i as u8 + 1); 15]);
        fs.release(reply.ino, fh, 0, None, true).await.unwrap();

        offset = offset.max(reply.offset);
    }

    assert!(offset > 0);

    let mut reply = Default::default();
    fs.readdir(dir_ino, dir_handle, offset, &mut reply).await.unwrap();
    assert_eq!(reply.entries.len(), 0);

    fs.releasedir(dir_ino, dir_handle, 0).await.unwrap();
}

#[tokio::test]
async fn test_lookup_negative_cached() {
    let fs_config = S3FilesystemConfig {
        cache_config: CacheConfig::new(TimeToLive::Duration(Duration::from_secs(600))),
        ..Default::default()
    };
    let (client, fs) = make_test_filesystem("test_lookup_negative_cached", &Default::default(), fs_config);

    let head_counter = client.new_counter(Operation::HeadObject);
    let list_counter = client.new_counter(Operation::ListObjectsV2);

    let _ = fs
        .lookup(FUSE_ROOT_INODE, "file1.txt".as_ref())
        .await
        .expect_err("should fail as no object exists");
    assert_eq!(head_counter.count(), 1);
    assert_eq!(list_counter.count(), 1);

    // Check negative caching
    let _ = fs
        .lookup(FUSE_ROOT_INODE, "file1.txt".as_ref())
        .await
        .expect_err("should fail as no object exists");
    assert_eq!(head_counter.count(), 1);
    assert_eq!(list_counter.count(), 1);

    client.add_object("file1.txt", MockObject::constant(0xa1, 15, ETag::for_tests()));

    let _ = fs
        .lookup(FUSE_ROOT_INODE, "file1.txt".as_ref())
        .await
        .expect_err("should fail as mountpoint should use negative cache");
    assert_eq!(head_counter.count(), 1);
    assert_eq!(list_counter.count(), 1);

    // Use readdirplus to discover the new file
    {
        let dir_handle = fs.opendir(FUSE_ROOT_INODE, 0).await.unwrap().fh;
        let mut reply = Default::default();
        fs.readdirplus(FUSE_ROOT_INODE, dir_handle, 0, &mut reply)
            .await
            .unwrap();

        assert_eq!(reply.entries.len(), 2 + 1);

        let mut entries = reply.entries.iter();
        let _ = entries.next().expect("should have current directory");
        let _ = entries.next().expect("should have parent directory");

        let entry = entries.next().expect("should have file1.txt in entries");
        let expected = OsString::from("file1.txt");
        assert_eq!(entry.name, expected);
        assert_eq!(entry.attr.kind, FileType::RegularFile);
    }
    assert_eq!(head_counter.count(), 1);
    assert_eq!(list_counter.count(), 2);

    let _ = fs
        .lookup(FUSE_ROOT_INODE, "file1.txt".as_ref())
        .await
        .expect("should succeed as object is cached and exists");
    assert_eq!(head_counter.count(), 1);
    assert_eq!(list_counter.count(), 2);
}

#[tokio::test]
async fn test_lookup_then_open_cached() {
    let fs_config = S3FilesystemConfig {
        cache_config: CacheConfig::new(TimeToLive::Duration(Duration::from_secs(600))),
        ..Default::default()
    };
    let (client, fs) = make_test_filesystem("test_lookup_then_open_cached", &Default::default(), fs_config);

    client.add_object("file1.txt", MockObject::constant(0xa1, 15, ETag::for_tests()));

    let head_counter = client.new_counter(Operation::HeadObject);
    let list_counter = client.new_counter(Operation::ListObjectsV2);

    let entry = fs.lookup(FUSE_ROOT_INODE, "file1.txt".as_ref()).await.unwrap();
    let ino = entry.attr.ino;
    assert_eq!(head_counter.count(), 1);
    assert_eq!(list_counter.count(), 1);

    let fh = fs.open(ino, OpenFlags::empty(), 0).await.unwrap().fh;
    fs.release(ino, fh, 0, None, true).await.unwrap();
    assert_eq!(head_counter.count(), 1);
    assert_eq!(list_counter.count(), 1);

    let fh = fs.open(entry.attr.ino, OpenFlags::empty(), 0).await.unwrap().fh;
    fs.release(ino, fh, 0, None, true).await.unwrap();
    assert_eq!(head_counter.count(), 1);
    assert_eq!(list_counter.count(), 1);
}

#[tokio::test]
async fn test_lookup_then_open_no_cache() {
    let fs_config = S3FilesystemConfig {
        cache_config: CacheConfig::new(TimeToLive::Minimal),
        ..Default::default()
    };
    let (client, fs) = make_test_filesystem("test_lookup_then_open_no_cache", &Default::default(), fs_config);

    client.add_object("file1.txt", MockObject::constant(0xa1, 15, ETag::for_tests()));

    let head_counter = client.new_counter(Operation::HeadObject);
    let list_counter = client.new_counter(Operation::ListObjectsV2);

    let entry = fs.lookup(FUSE_ROOT_INODE, "file1.txt".as_ref()).await.unwrap();
    let ino = entry.attr.ino;
    assert_eq!(head_counter.count(), 1);
    assert_eq!(list_counter.count(), 1);

    let fh = fs.open(ino, OpenFlags::empty(), 0).await.unwrap().fh;
    fs.release(ino, fh, 0, None, true).await.unwrap();
    assert_eq!(head_counter.count(), 2);
    assert_eq!(list_counter.count(), 2);

    let fh = fs.open(entry.attr.ino, OpenFlags::empty(), 0).await.unwrap().fh;
    fs.release(ino, fh, 0, None, true).await.unwrap();
    assert_eq!(head_counter.count(), 3);
    assert_eq!(list_counter.count(), 3);
}

#[tokio::test]
async fn test_readdir_then_open_cached() {
    let fs_config = S3FilesystemConfig {
        cache_config: CacheConfig::new(TimeToLive::Duration(Duration::from_secs(600))),
        ..Default::default()
    };
    let (client, fs) = make_test_filesystem("test_readdir_then_open_cached", &Default::default(), fs_config);

    client.add_object("file1.txt", MockObject::constant(0xa1, 15, ETag::for_tests()));

    // Repeat to check readdir is not currently served from cache
    for _ in 0..2 {
        let head_counter = client.new_counter(Operation::HeadObject);
        let list_counter = client.new_counter(Operation::ListObjectsV2);

        let dir_ino = FUSE_ROOT_INODE;
        let dir_handle = fs.opendir(dir_ino, 0).await.unwrap().fh;
        let mut reply = Default::default();
        fs.readdirplus(dir_ino, dir_handle, 0, &mut reply).await.unwrap();

        assert_eq!(reply.entries.len(), 2 + 1);

        let mut entries = reply.entries.iter();
        let _ = entries.next().expect("should have current directory");
        let _ = entries.next().expect("should have parent directory");

        let entry = entries.next().expect("should have file1.txt in entries");
        let expected = OsString::from("file1.txt");
        assert_eq!(entry.name, expected);
        assert_eq!(entry.attr.kind, FileType::RegularFile);

        assert_eq!(head_counter.count(), 0);
        assert_eq!(list_counter.count(), 1);

        let fh = fs.open(entry.ino, OpenFlags::empty(), 0).await.unwrap().fh;

        assert_eq!(head_counter.count(), 0);
        assert_eq!(list_counter.count(), 1);
        fs.release(entry.ino, fh, 0, None, true).await.unwrap();
        fs.releasedir(dir_ino, dir_handle, 0).await.unwrap();

        assert_eq!(head_counter.count(), 0);
        assert_eq!(list_counter.count(), 1);
    }
}

#[tokio::test]
async fn test_unlink_cached() {
    let fs_config = S3FilesystemConfig {
        cache_config: CacheConfig::new(TimeToLive::Duration(Duration::from_secs(600))),
        allow_delete: true,
        ..Default::default()
    };
    let (client, fs) = make_test_filesystem("test_lookup_then_open_cached", &Default::default(), fs_config);

    client.add_object("file1.txt", MockObject::constant(0xa1, 15, ETag::for_tests()));

    let parent_ino = FUSE_ROOT_INODE;
    let head_counter = client.new_counter(Operation::HeadObject);
    let list_counter = client.new_counter(Operation::ListObjectsV2);

    let _entry = fs
        .lookup(parent_ino, "file1.txt".as_ref())
        .await
        .expect("should find file as object exists");
    assert_eq!(head_counter.count(), 1);
    assert_eq!(list_counter.count(), 1);

    client.remove_object("file1.txt");
    let _entry = fs
        .lookup(parent_ino, "file1.txt".as_ref())
        .await
        .expect("lookup should still see obj");
    assert_eq!(head_counter.count(), 1);
    assert_eq!(list_counter.count(), 1);

    fs.unlink(parent_ino, "file1.txt".as_ref())
        .await
        .expect("unlink should unlink cached object");
    assert_eq!(head_counter.count(), 1);
    assert_eq!(list_counter.count(), 1);

    let _entry = fs
        .lookup(parent_ino, "file1.txt".as_ref())
        .await
        .expect_err("cached entry should now be gone");
    assert_eq!(head_counter.count(), 2);
    assert_eq!(list_counter.count(), 2);
}

#[tokio::test]
async fn test_mknod_cached() {
    const BUCKET_NAME: &str = "test_mknod_cached";
    let fs_config = S3FilesystemConfig {
        cache_config: CacheConfig::new(TimeToLive::Duration(Duration::from_secs(600))),
        ..Default::default()
    };
    let (client, fs) = make_test_filesystem(BUCKET_NAME, &Default::default(), fs_config);

    let parent = FUSE_ROOT_INODE;
    let head_counter = client.new_counter(Operation::HeadObject);
    let list_counter = client.new_counter(Operation::ListObjectsV2);

    client.add_object("file1.txt", MockObject::constant(0xa1, 15, ETag::for_tests()));

    let mode = libc::S_IFREG | libc::S_IRWXU; // regular file + 0700 permissions
    let err_no = fs
        .mknod(parent, "file1.txt".as_ref(), mode, 0, 0)
        .await
        .expect_err("file already exists")
        .to_errno();
    assert_eq!(err_no, libc::EEXIST, "expected EEXIST but got {err_no:?}");
    assert_eq!(head_counter.count(), 1);
    assert_eq!(list_counter.count(), 1);

    client.remove_object("file1.txt");

    let err_no = fs
        .mknod(parent, "file1.txt".as_ref(), mode, 0, 0)
        .await
        .expect_err("should fail as directory entry still cached")
        .to_errno();
    assert_eq!(err_no, libc::EEXIST, "expected EEXIST but got {err_no:?}");
    assert_eq!(head_counter.count(), 1);
    assert_eq!(list_counter.count(), 1);
}

#[test_case(1024 * 1024; "small")]
#[test_case(50 * 1024 * 1024; "large")]
#[tokio::test]
async fn test_random_read(object_size: usize) {
    let (client, fs) = make_test_filesystem("test_random_read", &Default::default(), Default::default());

    let mut rng = ChaCha20Rng::seed_from_u64(0x12345678 + object_size as u64);
    let mut expected = vec![0; object_size];
    rng.fill(&mut expected[..]);
    client.add_object("file", MockObject::from_bytes(&expected[..], ETag::for_tests()));

    // Find the object
    let dir_handle = fs.opendir(FUSE_ROOT_INODE, 0).await.unwrap().fh;
    let mut reply = Default::default();
    fs.readdirplus(1, dir_handle, 0, &mut reply).await.unwrap();

    assert_eq!(reply.entries.len(), 2 + 1);

    assert_eq!(reply.entries[2].name, "file");
    let ino = reply.entries[2].ino;

    let fh = fs.open(ino, OpenFlags::empty(), 0).await.unwrap().fh;

    let mut rng = ChaCha20Rng::seed_from_u64(0x12345678);
    for _ in 0..10 {
        let offset = rng.gen_range(0..object_size);
        // TODO do we need to bound it? should work anyway, just partial read, right?
        let length = rng.gen_range(0..(object_size - offset).min(1024 * 1024)) + 1;
        let bytes_read = fs
            .read(ino, fh, offset as i64, length as u32, 0, None)
            .await
            .expect("fs read should succeed");
        assert_eq!(bytes_read.len(), length);
        assert_eq!(&bytes_read[..], &expected[offset..offset + length]);
    }

    fs.release(ino, fh, 0, None, true).await.unwrap();
    fs.releasedir(FUSE_ROOT_INODE, dir_handle, 0).await.unwrap();
}

#[test_case(""; "unprefixed")]
#[test_case("test_prefix/"; "prefixed")]
#[tokio::test]
async fn test_implicit_directory_shadow(prefix: &str) {
    let prefix = Prefix::new(prefix).expect("valid prefix");
    let (client, fs) = make_test_filesystem("test_implicit_directory_shadow", &prefix, Default::default());

    // Make an object that matches a directory name. We want this object to be shadowed by the
    // directory.
    client.add_object(
        &format!("{prefix}dir1/"),
        MockObject::constant(0xa1, 15, ETag::from_str("test_etag_1").unwrap()),
    );
    client.add_object(
        &format!("{prefix}dir1/file2.txt"),
        MockObject::constant(0xa2, 15, ETag::from_str("test_etag_2").unwrap()),
    );

    let entry = fs.lookup(FUSE_ROOT_INODE, "dir1".as_ref()).await.unwrap();
    assert_eq!(entry.attr.kind, FileType::Directory);
    let dir_ino = entry.attr.ino;

    let dir_handle = fs.opendir(dir_ino, 0).await.unwrap().fh;
    let mut reply = Default::default();
    fs.readdirplus(dir_ino, dir_handle, 0, &mut reply).await.unwrap();

    assert_eq!(reply.entries.len(), 2 + 1);

    assert_eq!(reply.entries[0].name, ".");
    assert_eq!(reply.entries[0].ino, dir_ino);
    assert_eq!(reply.entries[1].name, "..");
    assert_eq!(reply.entries[1].ino, FUSE_ROOT_INODE);

    assert_eq!(reply.entries[2].name, "file2.txt");
    assert_eq!(reply.entries[2].attr.kind, FileType::RegularFile);

    let fh = fs.open(reply.entries[2].ino, OpenFlags::empty(), 0).await.unwrap().fh;
    let bytes_read = fs
        .read(reply.entries[2].ino, fh, 0, 4096, 0, None)
        .await
        .expect("fs read should succeed");
    assert_eq!(&bytes_read[..], &[0xa2; 15]);
    fs.release(reply.entries[2].ino, fh, 0, None, true).await.unwrap();

    // Explicitly looking up the shadowed file should fail
    let entry = fs.lookup(FUSE_ROOT_INODE, "dir1/".as_ref()).await;
    assert!(matches!(entry, Err(e) if e.to_errno() == libc::EINVAL));

    // TODO test removing the directory, removing the file

    fs.releasedir(dir_ino, dir_handle, 0).await.unwrap();
}

#[test_case(1024; "small")]
#[test_case(50 * 1024; "large")]
#[tokio::test]
async fn test_sequential_write(write_size: usize) {
    const BUCKET_NAME: &str = "test_sequential_write";
    const OBJECT_SIZE: usize = 50 * 1024;

    let (client, fs) = make_test_filesystem(BUCKET_NAME, &Default::default(), Default::default());

    let mut rng = ChaCha20Rng::seed_from_u64(0x12345678 + OBJECT_SIZE as u64);
    let mut body = vec![0u8; OBJECT_SIZE];
    rng.fill(&mut body[..]);

    client.add_object("dir1/file1.bin", MockObject::constant(0xa1, 15, ETag::for_tests()));

    // Find the dir1 directory
    let entry = fs.lookup(FUSE_ROOT_INODE, "dir1".as_ref()).await.unwrap();
    assert_eq!(entry.attr.kind, FileType::Directory);
    let dir_ino = entry.attr.ino;

    // Write the object into that directory
    let mode = libc::S_IFREG | libc::S_IRWXU; // regular file + 0700 permissions
    let dentry = fs.mknod(dir_ino, "file2.bin".as_ref(), mode, 0, 0).await.unwrap();
    assert_eq!(dentry.attr.size, 0);
    let file_ino = dentry.attr.ino;

    let fh = fs.open(file_ino, OpenFlags::O_WRONLY, 0).await.unwrap().fh;

    let mut offset = 0;
    for data in body.chunks(write_size) {
        let written = fs.write(file_ino, fh, offset, data, 0, 0, None).await.unwrap();
        assert_eq!(written as usize, data.len());
        offset += written as i64;
    }

    fs.release(file_ino, fh, 0, None, false).await.unwrap();

    // Check that the object made it to S3 as we expected
    let get = client
        .get_object(BUCKET_NAME, "dir1/file2.bin", &GetObjectParams::new())
        .await
        .unwrap();
    let actual = get.collect().await.unwrap();
    assert_eq!(&actual[..], &body[..]);

    // And now check that we can read it out of the file system too. We need to emulate the kernel's
    // handling of ESTALE, which will retry after re-looking-up.
    let stat = match fs.getattr(file_ino).await {
        Ok(stat) => stat,
        Err(e) if e.to_errno() == libc::ESTALE => {
            let entry = fs.lookup(dir_ino, "file2.bin".as_ref()).await.unwrap();
            assert_ne!(entry.attr.ino, file_ino);
            fs.getattr(entry.attr.ino).await.unwrap()
        }
        Err(e) => panic!("unexpected getattr failure: {e:?}"),
    };
    assert_eq!(stat.attr.size, body.len() as u64);

    let dentry = fs.lookup(dir_ino, "file2.bin".as_ref()).await.unwrap();
    let size = dentry.attr.size as usize;
    assert_eq!(size, body.len());
    let file_ino = dentry.attr.ino;

    // First let's check that we can't write it again
    let result = fs
        .open(file_ino, OpenFlags::O_WRONLY, 0)
        .await
        .expect_err("file should not be overwritable")
        .to_errno();
    assert_eq!(result, libc::EPERM);

    // But read-only should work
    let fh = fs.open(file_ino, OpenFlags::empty(), 0).await.unwrap().fh;

    let mut offset = 0;
    while offset < size {
        let length = 1024.min(size - offset);
        let bytes_read = fs
            .read(file_ino, fh, offset as i64, length as u32, 0, None)
            .await
            .expect("fs read should succeed");
        assert_eq!(bytes_read.len(), length);
        assert_eq!(&bytes_read[..], &body[offset..offset + length]);
        offset += length;
    }

    fs.release(file_ino, fh, 0, None, true).await.unwrap();
}

#[test_case(-27; "earlier offset")]
#[test_case(28; "later offset")]
#[tokio::test]
async fn test_unordered_write_fails(offset: i64) {
    const BUCKET_NAME: &str = "test_unordered_write_fails";

    let (_client, fs) = make_test_filesystem(BUCKET_NAME, &Default::default(), Default::default());

    let mode = libc::S_IFREG | libc::S_IRWXU; // regular file + 0700 permissions
    let dentry = fs
        .mknod(FUSE_ROOT_INODE, "file2.bin".as_ref(), mode, 0, 0)
        .await
        .unwrap();
    assert_eq!(dentry.attr.size, 0);
    let file_ino = dentry.attr.ino;

    let fh = fs.open(file_ino, OpenFlags::O_WRONLY, 0).await.unwrap().fh;

    let slice = &[0xaa; 27];
    let written = fs.write(file_ino, fh, 0, slice, 0, 0, None).await.unwrap();
    assert_eq!(written, 27);

    let err = fs
        .write(file_ino, fh, written as i64 + offset, slice, 0, 0, None)
        .await
        .expect_err("writes to out-of-order offsets should fail")
        .to_errno();
    assert_eq!(err, libc::EINVAL);

    let err = fs
        .write(file_ino, fh, written as i64, slice, 0, 0, None)
        .await
        .expect_err("any write after an error should fail")
        .to_errno();
    assert_eq!(err, libc::EINVAL);
}

#[test_case(OpenFlags::O_SYNC; "O_SYNC")]
#[test_case(OpenFlags::O_DSYNC; "O_DSYNC")]
#[tokio::test]
async fn test_sync_flags_fail(flag: OpenFlags) {
    const BUCKET_NAME: &str = "test_sync_flags_fail";

    let (_client, fs) = make_test_filesystem(BUCKET_NAME, &Default::default(), Default::default());

    let mode = libc::S_IFREG | libc::S_IRWXU; // regular file + 0700 permissions
    let dentry = fs
        .mknod(FUSE_ROOT_INODE, "file2.bin".as_ref(), mode, 0, 0)
        .await
        .unwrap();
    let file_ino = dentry.attr.ino;

    let err = fs
        .open(file_ino, flag, 0)
        .await
        .expect_err("open should fail due to use of O_SYNC/O_DSYNC flag")
        .to_errno();
    assert_eq!(err, libc::EINVAL);
}

#[tokio::test]
async fn test_duplicate_write_fails() {
    const BUCKET_NAME: &str = "test_duplicate_write_fails";

    let (_client, fs) = make_test_filesystem(BUCKET_NAME, &Default::default(), Default::default());

    let mode = libc::S_IFREG | libc::S_IRWXU; // regular file + 0700 permissions
    let dentry = fs
        .mknod(FUSE_ROOT_INODE, "file2.bin".as_ref(), mode, 0, 0)
        .await
        .unwrap();
    assert_eq!(dentry.attr.size, 0);
    let file_ino = dentry.attr.ino;

    let _opened = fs.open(file_ino, OpenFlags::O_WRONLY, 0).await.unwrap();

    let err = fs
        .open(file_ino, OpenFlags::O_WRONLY, 0)
        .await
        .expect_err("should not be able to write twice")
        .to_errno();
    assert_eq!(err, libc::EPERM);
}

#[tokio::test]
async fn test_upload_aborted_on_write_failure() {
    const BUCKET_NAME: &str = "test_upload_aborted_on_write_failure";
    const FILE_NAME: &str = "foo.bin";

    let part_size = 1024 * 1024;
    let client = Arc::new(
        MockClient::config()
            .bucket(BUCKET_NAME)
            .part_size(part_size)
            .enable_backpressure(true)
            .initial_read_window_size(256 * 1024)
            .build(),
    );
    let mut put_failures = HashMap::new();
    put_failures.insert(1, Ok((2, MockClientError("error".to_owned().into()))));

    let failure_client = countdown_failure_client(
        client.clone(),
        CountdownFailureConfig {
            put_failures,
            ..Default::default()
        },
    );
    let fs = make_test_filesystem_with_client(
        Arc::new(failure_client),
        PagedPool::new_with_candidate_sizes([part_size]),
        BUCKET_NAME,
        &Default::default(),
        Default::default(),
    );

    let mode = libc::S_IFREG | libc::S_IRWXU; // regular file + 0700 permissions
    let dentry = fs.mknod(FUSE_ROOT_INODE, FILE_NAME.as_ref(), mode, 0, 0).await.unwrap();
    assert_eq!(dentry.attr.size, 0);
    let file_ino = dentry.attr.ino;

    let fh = fs.open(file_ino, OpenFlags::O_WRONLY, 0).await.unwrap().fh;

    let written = fs
        .write(file_ino, fh, 0, &[0xaa; 27], 0, 0, None)
        .await
        .expect("first write should succeed");

    assert!(client.is_upload_in_progress(FILE_NAME));

    let write_error = fs
        .write(file_ino, fh, written as i64, &[0xaa; 27], 0, 0, None)
        .await
        .expect_err("second write should fail")
        .to_errno();
    assert_eq!(write_error, libc::EIO);

    let err = fs
        .write(file_ino, fh, 0, &[0xaa; 27], 0, 0, None)
        .await
        .expect_err("subsequent writes should fail")
        .to_errno();

    assert_eq!(err, libc::EIO);

    assert!(!client.is_upload_in_progress(FILE_NAME));
    assert!(!client.contains_key(FILE_NAME));

    let err = fs
        .fsync(file_ino, fh, true)
        .await
        .expect_err("subsequent fsync should fail")
        .to_errno();
    assert_eq!(err, libc::EIO);

    fs.release(file_ino, fh, 0, None, true)
        .await
        .expect("release succeeds (no op)");
}

#[tokio::test]
async fn test_upload_aborted_on_fsync_failure() {
    const BUCKET_NAME: &str = "test_upload_aborted_on_fsync_failure";
    const FILE_NAME: &str = "foo.bin";

    let part_size = 1024 * 1024;
    let client = Arc::new(
        MockClient::config()
            .bucket(BUCKET_NAME)
            .part_size(part_size)
            .enable_backpressure(true)
            .initial_read_window_size(256 * 1024)
            .build(),
    );
    let mut put_failures = HashMap::new();
    put_failures.insert(1, Ok((2, MockClientError("error".to_owned().into()))));

    let failure_client = countdown_failure_client(
        client.clone(),
        CountdownFailureConfig {
            put_failures,
            ..Default::default()
        },
    );
    let fs = make_test_filesystem_with_client(
        Arc::new(failure_client),
        PagedPool::new_with_candidate_sizes([part_size]),
        BUCKET_NAME,
        &Default::default(),
        Default::default(),
    );

    let mode = libc::S_IFREG | libc::S_IRWXU; // regular file + 0700 permissions
    let dentry = fs.mknod(FUSE_ROOT_INODE, FILE_NAME.as_ref(), mode, 0, 0).await.unwrap();
    assert_eq!(dentry.attr.size, 0);
    let file_ino = dentry.attr.ino;

    let fh = fs.open(file_ino, OpenFlags::O_WRONLY, 0).await.unwrap().fh;

    _ = fs
        .write(file_ino, fh, 0, &[0xaa; 27], 0, 0, None)
        .await
        .expect("first write should succeed");

    assert!(client.is_upload_in_progress(FILE_NAME));

    let err = fs
        .fsync(file_ino, fh, true)
        .await
        .expect_err("subsequent fsync should fail")
        .to_errno();
    assert_eq!(err, libc::EIO);

    assert!(!client.is_upload_in_progress(FILE_NAME));
    assert!(!client.contains_key(FILE_NAME));

    fs.release(file_ino, fh, 0, None, true)
        .await
        .expect("release succeeds (no op)");
}

#[tokio::test]
async fn test_upload_aborted_on_release_failure() {
    const BUCKET_NAME: &str = "test_upload_aborted_on_fsync_failure";
    const FILE_NAME: &str = "foo.bin";

    let part_size = 1024 * 1024;
    let client = Arc::new(
        MockClient::config()
            .bucket(BUCKET_NAME)
            .part_size(part_size)
            .enable_backpressure(true)
            .initial_read_window_size(256 * 1024)
            .build(),
    );
    let mut put_failures = HashMap::new();
    put_failures.insert(1, Ok((2, MockClientError("error".to_owned().into()))));

    let failure_client = countdown_failure_client(
        client.clone(),
        CountdownFailureConfig {
            put_failures,
            ..Default::default()
        },
    );
    let fs = make_test_filesystem_with_client(
        Arc::new(failure_client),
        PagedPool::new_with_candidate_sizes([part_size]),
        BUCKET_NAME,
        &Default::default(),
        Default::default(),
    );

    let mode = libc::S_IFREG | libc::S_IRWXU; // regular file + 0700 permissions
    let dentry = fs.mknod(FUSE_ROOT_INODE, FILE_NAME.as_ref(), mode, 0, 0).await.unwrap();
    assert_eq!(dentry.attr.size, 0);
    let file_ino = dentry.attr.ino;

    let fh = fs.open(file_ino, OpenFlags::O_WRONLY, 0).await.unwrap().fh;

    _ = fs
        .write(file_ino, fh, 0, &[0xaa; 27], 0, 0, None)
        .await
        .expect("first write should succeed");

    assert!(client.is_upload_in_progress(FILE_NAME));

    let err = fs
        .release(file_ino, fh, 0, None, true)
        .await
        .expect_err("subsequent release should fail")
        .to_errno();
    assert_eq!(err, libc::EIO);

    assert!(!client.is_upload_in_progress(FILE_NAME));
    assert!(!client.contains_key(FILE_NAME));
}

#[tokio::test]
async fn test_stat_block_size() {
    let (client, fs) = make_test_filesystem("test_stat_block_size", &Default::default(), Default::default());

    client.add_object(
        "file0.txt",
        MockObject::constant(0xa1, 0, ETag::from_str("test_etag_1").unwrap()),
    );
    client.add_object(
        "file1.txt",
        MockObject::constant(0xa2, 1, ETag::from_str("test_etag_2").unwrap()),
    );
    client.add_object(
        "file4096.txt",
        MockObject::constant(0xa3, 4096, ETag::from_str("test_etag_3").unwrap()),
    );
    client.add_object(
        "file4097.txt",
        MockObject::constant(0xa3, 4097, ETag::from_str("test_etag_4").unwrap()),
    );

    let lookup = fs.lookup(FUSE_ROOT_INODE, "file0.txt".as_ref()).await.unwrap();
    assert_eq!(lookup.attr.blocks, 0);
    assert_eq!(lookup.attr.blksize, 4096);

    let lookup = fs.lookup(FUSE_ROOT_INODE, "file1.txt".as_ref()).await.unwrap();
    assert_eq!(lookup.attr.blocks, 1);
    assert_eq!(lookup.attr.blksize, 4096);

    let lookup = fs.lookup(FUSE_ROOT_INODE, "file4096.txt".as_ref()).await.unwrap();
    assert_eq!(lookup.attr.blocks, 8);
    assert_eq!(lookup.attr.blksize, 4096);

    let lookup = fs.lookup(FUSE_ROOT_INODE, "file4097.txt".as_ref()).await.unwrap();
    assert_eq!(lookup.attr.blocks, 9);
    assert_eq!(lookup.attr.blksize, 4096);
}

#[test_case("foo"; "remove file")]
#[test_case("bar/foo"; "remove directory")]
#[tokio::test]
async fn test_lookup_removes_old_children(key: &str) {
    let (client, fs) = make_test_filesystem(
        "test_lookup_removes_old_children",
        &Default::default(),
        Default::default(),
    );

    client.add_object(key, MockObject::constant(0xa1, 0, ETag::for_tests()));

    let child_name = key.split_once('/').map(|(p, _)| p).unwrap_or(key);

    // Ensure the file is visible in mountpoint
    fs.lookup(FUSE_ROOT_INODE, child_name.as_ref()).await.unwrap();

    // Remove object on the client
    client.remove_object(key);

    fs.lookup(FUSE_ROOT_INODE, child_name.as_ref())
        .await
        .expect_err("the child should not be visible");

    fs.mknod(
        FUSE_ROOT_INODE,
        child_name.as_ref(),
        libc::S_IFREG | libc::S_IRWXU,
        0,
        0,
    )
    .await
    .expect("should create a new child with the same name");
}

#[test_case(""; "unprefixed")]
#[test_case("test_prefix/"; "prefixed")]
#[tokio::test]
async fn test_local_dir(prefix: &str) {
    let prefix = Prefix::new(prefix).expect("valid prefix");
    let (client, fs) = make_test_filesystem("test_local_dir", &prefix, Default::default());

    // Create local directory
    let dirname = "local";
    let dir_entry = fs
        .mkdir(FUSE_ROOT_INODE, dirname.as_ref(), libc::S_IFDIR, 0)
        .await
        .unwrap();

    assert_eq!(dir_entry.attr.kind, FileType::Directory);
    let dir_ino = dir_entry.attr.ino;

    assert!(!client.contains_prefix(&format!("{prefix}{dirname}")));

    let lookup_entry = fs.lookup(FUSE_ROOT_INODE, dirname.as_ref()).await.unwrap();
    assert_eq!(lookup_entry.attr, dir_entry.attr);

    // Write an object into the directory
    let filename = "file.bin";
    let mode = libc::S_IFREG | libc::S_IRWXU; // regular file + 0700 permissions
    let file_entry = fs.mknod(dir_ino, filename.as_ref(), mode, 0, 0).await.unwrap();
    let file_ino = file_entry.attr.ino;
    let file_handle = fs.open(file_ino, OpenFlags::O_WRONLY, 0).await.unwrap().fh;

    fs.release(file_ino, file_handle, 0, None, false).await.unwrap();

    // Remove the new object from the client
    client.remove_object(&format!("{prefix}{dirname}/{filename}"));

    // Verify that the directory disappeared
    let lookup = fs.lookup(FUSE_ROOT_INODE, dirname.as_ref()).await;
    assert!(matches!(lookup, Err(e) if e.to_errno() == libc::ENOENT));
}

#[tokio::test]
async fn test_directory_shadowing_lookup() {
    let (client, fs) = make_test_filesystem(
        "test_directory_shadowing_lookup",
        &Default::default(),
        Default::default(),
    );

    // Add an object
    let name = "foo";
    client.add_object(name, b"foo".into());

    let lookup_entry = fs.lookup(FUSE_ROOT_INODE, name.as_ref()).await.unwrap();
    assert_eq!(lookup_entry.attr.kind, FileType::RegularFile);

    // Add another object, whose prefix shadows the first
    let nested = format!("{name}/bar");
    client.add_object(&nested, b"bar".into());

    let lookup_entry = fs.lookup(FUSE_ROOT_INODE, name.as_ref()).await.unwrap();
    assert_eq!(lookup_entry.attr.kind, FileType::Directory);

    // Remove the second object
    client.remove_object(&nested);

    let lookup_entry = fs.lookup(FUSE_ROOT_INODE, name.as_ref()).await.unwrap();
    assert_eq!(lookup_entry.attr.kind, FileType::RegularFile);
}

#[tokio::test]
async fn test_directory_shadowing_readdir() {
    let (client, fs) = make_test_filesystem(
        "test_directory_shadowing_readdir",
        &Default::default(),
        Default::default(),
    );

    // Add `foo/bar` as a file
    client.add_object("foo/bar", b"foo/bar".into());

    let foo_dir = fs.lookup(FUSE_ROOT_INODE, "foo".as_ref()).await.unwrap();
    assert_eq!(foo_dir.attr.kind, FileType::Directory);

    let bar_dentry = {
        let dir_handle = fs.opendir(foo_dir.attr.ino, 0).await.unwrap().fh;
        let mut reply = Default::default();
        fs.readdir(foo_dir.attr.ino, dir_handle, 0, &mut reply).await.unwrap();
        fs.releasedir(foo_dir.attr.ino, dir_handle, 0).await.unwrap();

        // Skip . and .. to get to the `bar` dentry
        reply.entries.get(2).unwrap().clone()
    };
    // The `bar` dentry should be a file
    assert_eq!(bar_dentry.attr.kind, FileType::RegularFile);
    assert_eq!(bar_dentry.name, "bar");

    // Lookup should be consistent with readdir
    let bar_file = fs.lookup(foo_dir.attr.ino, "bar".as_ref()).await.unwrap();
    assert_eq!(bar_file.attr.kind, FileType::RegularFile);
    assert_eq!(bar_file.attr.ino, bar_dentry.attr.ino);

    // Add another object that shadows the first `bar` file with a directory
    client.add_object("foo/bar/baz", b"bar".into());

    let bar_dentry_new = {
        let dir_handle = fs.opendir(foo_dir.attr.ino, 0).await.unwrap().fh;
        let mut reply = Default::default();
        fs.readdir(bar_file.attr.ino, dir_handle, 0, &mut reply).await.unwrap();
        fs.releasedir(bar_file.attr.ino, dir_handle, 0).await.unwrap();

        // Skip . and .. to get to the `bar` dentry
        reply.entries.get(2).unwrap().clone()
    };
    // The `bar` dentry should now be a directory and a different
    // inode to the original `bar`
    assert_eq!(bar_dentry_new.attr.kind, FileType::Directory);
    assert_eq!(bar_dentry.name, "bar");
    assert_ne!(bar_dentry_new.attr.ino, bar_dentry.attr.ino);

    // Lookup should again be consistent with readdir
    let bar_dir = fs.lookup(foo_dir.attr.ino, "bar".as_ref()).await.unwrap();
    assert_eq!(bar_dir.attr.kind, FileType::Directory);
    assert_eq!(bar_dir.attr.ino, bar_dentry_new.attr.ino);

    // Remove the second object, revealing the original `bar` file again
    client.remove_object("foo/bar/baz");

    let bar_dentry = {
        let dir_handle = fs.opendir(foo_dir.attr.ino, 0).await.unwrap().fh;
        let mut reply = Default::default();
        fs.readdir(foo_dir.attr.ino, dir_handle, 0, &mut reply).await.unwrap();
        fs.releasedir(foo_dir.attr.ino, dir_handle, 0).await.unwrap();

        // Skip . and .. to get to the `bar` dentry
        reply.entries.get(2).unwrap().clone()
    };
    // The `bar` dentry should be a file again and a different inode to
    // the `bar` directory above that's now gone. We're ambivalent about
    // whether it's the same inode as the original file we saw above.
    assert_eq!(bar_dentry.attr.kind, FileType::RegularFile);
    assert_eq!(bar_dentry.name, "bar");
    assert_ne!(bar_dentry.attr.ino, bar_dentry_new.attr.ino);

    // Lookup should be consistent with readdir
    let bar_file = fs.lookup(foo_dir.attr.ino, "bar".as_ref()).await.unwrap();
    assert_eq!(bar_file.attr.kind, FileType::RegularFile);
    assert_eq!(bar_file.attr.ino, bar_dentry.attr.ino);
}

#[tokio::test]
async fn test_readdir_vs_readdirplus() {
    let (client, fs) = make_test_filesystem("test_readdir_vs_readdirplus", &Default::default(), Default::default());

    client.add_object("bar", b"bar".into());
    client.add_object("baz/foo", b"foo".into());

    let readdir_entries = {
        let dir_handle = fs.opendir(FUSE_ROOT_INODE, 0).await.unwrap().fh;
        let mut reply = Default::default();
        fs.readdir(FUSE_ROOT_INODE, dir_handle, 0, &mut reply).await.unwrap();
        fs.releasedir(FUSE_ROOT_INODE, dir_handle, 0).await.unwrap();

        // Skip . and ..
        reply.entries.into_iter().skip(2).collect::<Vec<_>>()
    };

    assert_eq!(
        readdir_entries.iter().map(|e| &e.name).collect::<Vec<_>>(),
        &["bar", "baz"]
    );

    for entry in readdir_entries {
        let err = fs
            .getattr(entry.ino)
            .await
            .expect_err("readdir should not add inodes to the superblock")
            .to_errno();
        assert!(matches!(err, libc::ENOENT));
    }

    let readdirplus_entries = {
        let dir_handle = fs.opendir(FUSE_ROOT_INODE, 0).await.unwrap().fh;
        let mut reply = Default::default();
        fs.readdirplus(FUSE_ROOT_INODE, dir_handle, 0, &mut reply)
            .await
            .unwrap();
        fs.releasedir(FUSE_ROOT_INODE, dir_handle, 0).await.unwrap();

        // Skip . and ..
        reply.entries.into_iter().skip(2).collect::<Vec<_>>()
    };

    assert_eq!(
        readdirplus_entries.iter().map(|e| &e.name).collect::<Vec<_>>(),
        &["bar", "baz"]
    );

    for entry in readdirplus_entries {
        let attr = fs
            .getattr(entry.ino)
            .await
            .expect("readdirplus should add inodes to the superblock");
        assert_eq!(entry.ino, attr.attr.ino);
    }
}

#[tokio::test]
async fn test_flexible_retrieval_objects() {
    const NAMES: &[&str] = &[
        "GLACIER",
        "GLACIER_IR",
        "DEEP_ARCHIVE",
        "GLACIER_RESTORED",
        "DEEP_ARCHIVE_RESTORED",
    ];

    let (client, fs) = make_test_filesystem(
        "test_flexible_retrieval_objects",
        &Default::default(),
        Default::default(),
    );

    for name in NAMES {
        let mut object = MockObject::from(b"hello world");
        object.set_storage_class(Some(name.to_string().replace("_RESTORED", "")));
        object.set_restored(if name.contains("_RESTORED") {
            Some(RestoreStatus::Restored {
                expiry: SystemTime::now().add(Duration::from_secs(3600)),
            })
        } else {
            None
        });
        client.add_object(name, object);
    }

    let dir_handle = fs.opendir(FUSE_ROOT_INODE, 0).await.unwrap().fh;
    let mut reply = Default::default();
    fs.readdirplus(FUSE_ROOT_INODE, dir_handle, 0, &mut reply)
        .await
        .unwrap();
    fs.releasedir(FUSE_ROOT_INODE, dir_handle, 0).await.unwrap();

    // Skip . and ..
    let iter = reply.entries.into_iter().skip(2);
    for entry in iter {
        let flexible_retrieval = entry.name == "GLACIER" || entry.name == "DEEP_ARCHIVE";
        assert_eq!(flexible_retrieval, entry.attr.perm == 0);

        let getattr = fs.getattr(entry.ino).await.unwrap();
        assert_eq!(flexible_retrieval, getattr.attr.perm == 0);

        let open = fs.open(entry.ino, OpenFlags::empty(), 0).await;
        if flexible_retrieval {
            let err = open.expect_err("can't open flexible retrieval objects");
            assert_eq!(err.to_errno(), libc::EACCES);
        } else {
            let open = open.expect("instant retrieval files are readable");
            fs.release(entry.ino, open.fh, 0, None, true).await.unwrap();
        }
    }

    // Try via the non-readdir path
    for name in NAMES {
        let flexible_retrieval = *name == "GLACIER" || *name == "DEEP_ARCHIVE";

        let file_name = format!("{name}2");
        let mut object = MockObject::from(b"hello world");
        object.set_storage_class(Some(name.to_string().replace("_RESTORED", "")));
        object.set_restored(if name.contains("_RESTORED") {
            Some(RestoreStatus::Restored {
                expiry: SystemTime::now().add(Duration::from_secs(3600)),
            })
        } else {
            None
        });
        client.add_object(&file_name, object);

        let lookup = fs.lookup(FUSE_ROOT_INODE, file_name.as_ref()).await.unwrap();

        let getattr = fs.getattr(lookup.attr.ino).await.unwrap();
        assert_eq!(flexible_retrieval, getattr.attr.perm == 0);

        let open = fs.open(lookup.attr.ino, OpenFlags::empty(), 0).await;
        if flexible_retrieval {
            let err = open.expect_err("can't open flexible retrieval objects");
            assert_eq!(err.to_errno(), libc::EACCES);
        } else {
            let open = open.expect("instant retrieval files are readable");
            fs.release(lookup.attr.ino, open.fh, 0, None, true).await.unwrap();
        }
    }
}

#[tokio::test]
async fn test_readdir_rewind_ordered() {
    let (client, fs) = make_test_filesystem("test_readdir_rewind", &Default::default(), Default::default());

    for i in 0..10 {
        client.add_object(&format!("foo{i}"), b"foo".into());
    }

    let dir_handle = fs.opendir(FUSE_ROOT_INODE, 0).await.unwrap().fh;

    let mut reply = DirectoryReply::new(5);
    fs.readdirplus(FUSE_ROOT_INODE, dir_handle, 0, &mut reply)
        .await
        .unwrap();
    let entries = reply
        .entries
        .iter()
        .map(|e| (e.ino, e.name.clone()))
        .collect::<Vec<_>>();
    assert_eq!(entries.len(), 5);

    // Trying to read out of order should fail (only offsets in the range of the previous response, or the one immediately following, are valid)
    assert!(reply.entries.back().unwrap().offset > 1);
    fs.readdirplus(FUSE_ROOT_INODE, dir_handle, 6, &mut Default::default())
        .await
        .expect_err("out of order");

    // Requesting the same buffer size should work fine
    let new_entries = ls(&fs, dir_handle, 0, 5).await;
    assert_eq!(entries, new_entries);

    // Requesting a smaller buffer works fine and returns a prefix
    let new_entries = ls(&fs, dir_handle, 0, 3).await;
    assert_eq!(&entries[..3], new_entries);

    // Requesting same offset (non zero) works fine by returning last response
    let _ = ls(&fs, dir_handle, 0, 5).await;
    let new_entries = ls(&fs, dir_handle, 5, 5).await;
    let new_entries_repeat = ls(&fs, dir_handle, 5, 5).await;
    assert_eq!(new_entries, new_entries_repeat);

    // Request all entries
    let new_entries = ls(&fs, dir_handle, 0, 20).await;
    assert_eq!(new_entries.len(), 12); // 10 files + 2 dirs (. and ..) = 12 entries

    // Request more entries but there is no more
    let new_entries = ls(&fs, dir_handle, 12, 20).await;
    assert_eq!(new_entries.len(), 0);

    // Request everything from zero one more time
    let new_entries = ls(&fs, dir_handle, 0, 20).await;
    assert_eq!(new_entries.len(), 12); // 10 files + 2 dirs (. and ..) = 12 entries

    // And we can resume the stream from the end of the first request
    // but let's rewind first
    let _ = ls(&fs, dir_handle, 0, 5).await;
    let mut next_page = DirectoryReply::new(0);
    fs.readdirplus(
        FUSE_ROOT_INODE,
        dir_handle,
        reply.entries.back().unwrap().offset,
        &mut next_page,
    )
    .await
    .unwrap();
    assert_eq!(next_page.entries.len(), 7); // 10 directory entries + . + .. = 12, minus the 5 we already saw
    assert_eq!(next_page.entries.front().unwrap().name, "foo3");

    for entry in reply.entries {
        // We know we're in the root dir, so the . and .. entries will both be FUSE_ROOT_INODE
        if entry.ino != FUSE_ROOT_INODE {
            // Each inode in this list should be remembered twice since we did two `readdirplus`es.
            // Forget will panic if this makes the lookup count underflow.
            fs.forget(entry.ino, 2).await;
        }
    }
}

#[tokio::test]
async fn test_readdir_rewind_unordered() {
    let config = S3FilesystemConfig {
        s3_personality: S3Personality::ExpressOneZone,
        ..Default::default()
    };
    let (client, fs) = make_test_filesystem("test_readdir_rewind", &Default::default(), config);

    for i in 0..10 {
        client.add_object(&format!("foo{i}"), b"foo".into());
    }

    let dir_handle = fs.opendir(FUSE_ROOT_INODE, 0).await.unwrap().fh;

    // Requesting same offset (non zero) works fine by returning last response
    let _ = ls(&fs, dir_handle, 0, 5).await;
    let new_entries = ls(&fs, dir_handle, 5, 5).await;
    let new_entries_repeat = ls(&fs, dir_handle, 5, 5).await;
    assert_eq!(new_entries, new_entries_repeat);

    // Request all entries
    let new_entries = ls(&fs, dir_handle, 0, 20).await;
    assert_eq!(new_entries.len(), 12); // 10 files + 2 dirs (. and ..) = 12 entries

    // Request more entries but there is no more
    let new_entries = ls(&fs, dir_handle, 12, 20).await;
    assert_eq!(new_entries.len(), 0);

    // Request everything from zero one more time
    let new_entries = ls(&fs, dir_handle, 0, 20).await;
    assert_eq!(new_entries.len(), 12); // 10 files + 2 dirs (. and ..) = 12 entries
}

#[test_case(Default::default())]
#[test_case(S3FilesystemConfig {s3_personality: S3Personality::ExpressOneZone, ..Default::default()})]
#[tokio::test]
async fn test_readdir_rewind_with_new_files(s3_fs_config: S3FilesystemConfig) {
    let (client, fs) = make_test_filesystem("test_readdir_rewind", &Default::default(), s3_fs_config);

    for i in 0..10 {
        client.add_object(&format!("foo{i}"), b"foo".into());
    }

    let dir_handle = fs.opendir(FUSE_ROOT_INODE, 0).await.unwrap().fh;

    // Let's add a new local file
    let file_name = "newfile.bin";
    new_local_file(&fs, file_name).await;

    // Let's add a new remote file
    client.add_object("foo10", b"foo".into());

    // Requesting same offset (non zero) works fine by returning last response
    let _ = ls(&fs, dir_handle, 0, 5).await;
    let new_entries = ls(&fs, dir_handle, 5, 5).await;
    let new_entries_repeat = ls(&fs, dir_handle, 5, 5).await;
    assert_eq!(new_entries, new_entries_repeat);

    // Request all entries
    let new_entries = ls(&fs, dir_handle, 0, 20).await;
    assert_eq!(new_entries.len(), 14); // 10 original remote files + 1 new local file + 1 new remote file + 2 dirs (. and ..) = 13 entries

    // assert entries contain new local file
    assert!(
        new_entries
            .iter()
            .any(|(_, name)| name.as_os_str().to_str().unwrap() == file_name)
    );

    // Request more entries but there is no more
    let new_entries = ls(&fs, dir_handle, 14, 20).await;
    assert_eq!(new_entries.len(), 0);

    // Request everything from zero one more time
    let new_entries = ls(&fs, dir_handle, 0, 20).await;
    assert_eq!(new_entries.len(), 14); // 10 original remote files + 1 new local file + 1 new remote file + 2 dirs (. and ..) = 13 entries
}

#[tokio::test]
async fn test_readdir_rewind_with_local_files_only() {
    let (_, fs) = make_test_filesystem("test_readdir_rewind", &Default::default(), Default::default());

    let dir_handle = fs.opendir(FUSE_ROOT_INODE, 0).await.unwrap().fh;

    // Let's add a new local file
    let file_name = "newfile.bin";
    new_local_file(&fs, file_name).await;

    // Requesting same offset (non zero) works fine by returning last response
    let _ = ls(&fs, dir_handle, 0, 5).await;
    let new_entries = ls(&fs, dir_handle, 3, 5).await;
    let new_entries_repeat = ls(&fs, dir_handle, 3, 5).await;
    assert_eq!(new_entries, new_entries_repeat);

    // Request all entries
    let new_entries = ls(&fs, dir_handle, 0, 20).await;
    assert_eq!(new_entries.len(), 3); // 1 new local file + 2 dirs (. and ..) = 3 entries

    // assert entries contain new local file
    assert!(new_entries.iter().any(|(_, name)| name == file_name));

    // Request more entries but there is no more
    let new_entries = ls(&fs, dir_handle, 3, 20).await;
    assert_eq!(new_entries.len(), 0);

    // Request everything from zero one more time
    let new_entries = ls(&fs, dir_handle, 0, 20).await;
    assert_eq!(new_entries.len(), 3); // 1 new local file + 2 dirs (. and ..) = 3 entries
}

// Check that request with an out-of-order offset which is in bounds of previously cached response is served well.
// This is relevant for situation when user application is interrupted in a readdir system call, which makes the
// kernel partially discard previous response and request some entries from it again:
//
// FUSE( 10) READDIRPLUS fh FileHandle(1), offset 0, size 4096
// FUSE( 11) INTERRUPT unique RequestId(10)
// FUSE( 12) READDIRPLUS fh FileHandle(1), offset 1, size 4096  <-- out-of-order offset `1`
// FUSE( 14) READDIRPLUS fh FileHandle(1), offset 25, size 4096
#[test_case(1, 25; "first in the beginning, second in full")]
#[test_case(24, 25; "first in the end, second in full")]
#[test_case(0, 26; "first in full, second in the beginning")]
#[test_case(0, 49; "first in full, second in the end")]
#[tokio::test]
async fn test_readdir_repeat_response_partial(first_repeated_offset: usize, second_repeated_offset: usize) {
    let (client, fs) = make_test_filesystem("test_readdir_repeat_response", &Default::default(), Default::default());

    for i in 0..48 {
        // "." and ".." make it a round 50 in total
        client.add_object(&format!("foo{i}"), b"foo".into());
    }

    let dir_handle = fs.opendir(FUSE_ROOT_INODE, 0).await.unwrap().fh;
    let max_entries = 25;

    // first request should just succeed
    let first_response = ls(&fs, dir_handle, 0, max_entries).await;
    assert!(first_response.len() == max_entries);

    // request some entries from the first response again
    let second_response = ls(&fs, dir_handle, first_repeated_offset as i64, max_entries).await;
    assert_eq!(&first_response[first_repeated_offset..], &second_response[..]);

    // read till the end
    let third_response = ls(&fs, dir_handle, 25, max_entries).await;
    assert!(third_response.len() == max_entries);

    // request some entries from the last response again
    let repeated_response = ls(&fs, dir_handle, second_repeated_offset as i64, max_entries).await;
    assert_eq!(&third_response[second_repeated_offset - 25..], &repeated_response[..]);

    // final response must be empty, signaling about EOF
    let final_response = ls(&fs, dir_handle, 50, max_entries).await;
    assert!(final_response.is_empty());
}

#[tokio::test]
async fn test_readdir_repeat_response_after_rewind() {
    let (client, fs) = make_test_filesystem("test_readdir_repeat_response", &Default::default(), Default::default());

    for i in 0..73 {
        // "." and ".." make it a round 75 in total
        client.add_object(&format!("foo{i}"), b"foo".into());
    }

    let dir_handle = fs.opendir(FUSE_ROOT_INODE, 0).await.unwrap().fh;
    let max_entries = 25;

    // read the first response, we'll later use it as an expected result
    let first_response = ls(&fs, dir_handle, 0, max_entries).await;
    assert!(first_response.len() == max_entries);

    // proceed in the stream, so we have a new response cached
    let second_response = ls(&fs, dir_handle, 25, max_entries).await;
    assert!(second_response.len() == max_entries);

    // ask for offset 0, causing a rewind (new S3 request)
    let rewinded_response = ls(&fs, dir_handle, 0, max_entries).await;
    assert_eq!(first_response, rewinded_response);

    // ask for offset 1, check that the correct cached response is used
    let repeated_response = ls(&fs, dir_handle, 1, max_entries).await;
    assert_eq!(&first_response[1..], repeated_response);
}

#[cfg(feature = "s3_tests")]
#[tokio::test]
async fn test_lookup_404_not_an_error() {
    let name = "test_lookup_404_not_an_error";
    let (bucket, prefix) = get_test_bucket_and_prefix(name);
    let part_size = 1024 * 1024;
    let pool = PagedPool::new_with_candidate_sizes([part_size]);
    let client_config = S3ClientConfig::default()
        .endpoint_config(get_test_endpoint_config())
        .read_backpressure(true)
        .memory_pool(pool.clone());
    let client = S3CrtClient::new(client_config).expect("must be able to create a CRT client");
    let fs = make_test_filesystem_with_client(
        client,
        pool,
        &bucket,
        &prefix.parse().expect("prefix must be valid"),
        Default::default(),
    );
    let err = fs
        .lookup(FUSE_ROOT_INODE, name.as_ref())
        .await
        .expect_err("lookup must fail");
    // ensure inexistent object has a dedicated error code
    assert_eq!(
        err.meta().error_code.as_deref(),
        Some(MOUNTPOINT_ERROR_LOOKUP_NONEXISTENT)
    );
}

// For S3 Express issues with permissions will in most cases be observed before the mount as it uses
// a single `s3express:CreateSession` action to grant access to ListObjectsV2 and all other operations.
// We perform ListObjectsV2 before the mount, which ensures that all further calls to S3 will be allowed
// unless a policy was modified while Mountpoint is running. We will test mount errors separately.
#[cfg(all(feature = "s3_tests", not(feature = "s3express_tests")))]
#[tokio::test]
async fn test_lookup_forbidden() {
    let name = "test_lookup_forbidden";
    let (bucket, prefix) = get_test_bucket_and_prefix(name);
    let key = format!("{}{}", prefix, name);
    let policy = deny_single_object_access_policy(&bucket, &key);

    let part_size = 1024 * 1024;
    let pool = PagedPool::new_with_candidate_sizes([part_size]);
    let auth_config = get_crt_client_auth_config(get_scoped_down_credentials(&policy).await);
    let client_config = S3ClientConfig::default()
        .auth_config(auth_config)
        .endpoint_config(get_test_endpoint_config())
        .read_backpressure(true)
        .memory_pool(pool.clone());
    let client = S3CrtClient::new(client_config).expect("must be able to create a CRT client");

    // create an empty file
    client
        .put_object(&bucket, &key, &Default::default())
        .await
        .expect("must be able to create a put object request")
        .complete()
        .await
        .expect("uploading an object must succeeed");

    // try to lookup file with a S3 policy that dissallows that
    let fs = make_test_filesystem_with_client(
        client,
        pool,
        &bucket,
        &prefix.parse().expect("prefix must be valid"),
        Default::default(),
    );
    let err = fs
        .lookup(FUSE_ROOT_INODE, name.as_ref())
        .await
        .expect_err("lookup must fail");
    let metadata = err.meta();
    assert_eq!(
        *metadata,
        ErrorMetadata {
            client_error_meta: ClientErrorMetadata {
                http_code: Some(403), // here we assume that HeadObject failes with 403 code
                error_code: None,
                error_message: None,
            },
            error_code: Some(MOUNTPOINT_ERROR_CLIENT.to_string()),
            s3_bucket_name: Some(bucket.to_string()),
            s3_object_key: Some(key.to_string())
        }
    );
}

async fn new_local_file(fs: &S3Filesystem<Arc<MockClient>>, filename: &str) {
    let mode = libc::S_IFREG | libc::S_IRWXU; // regular file + 0700 permissions
    let dentry = fs.mknod(FUSE_ROOT_INODE, filename.as_ref(), mode, 0, 0).await.unwrap();
    assert_eq!(dentry.attr.size, 0);
    let file_ino = dentry.attr.ino;

    let fh = fs.open(file_ino, OpenFlags::O_WRONLY, 0).await.unwrap().fh;

    let slice = &[0xaa; 27];
    let written = fs.write(file_ino, fh, 0, slice, 0, 0, None).await.unwrap();
    assert_eq!(written as usize, slice.len());
    fs.fsync(file_ino, fh, true).await.unwrap();
}

async fn ls(
    fs: &S3Filesystem<Arc<MockClient>>,
    dir_handle: u64,
    offset: i64,
    max_entries: usize,
) -> Vec<(u64, OsString)> {
    let mut reply = DirectoryReply::new(max_entries);
    fs.readdirplus(FUSE_ROOT_INODE, dir_handle, offset, &mut reply)
        .await
        .unwrap();
    reply
        .entries
        .iter()
        .map(|e| (e.ino, e.name.clone()))
        .collect::<Vec<_>>()
}

#[tokio::test]
async fn test_rename_support_is_cached() {
    const BUCKET_NAME: &str = "test_rename_support_cached_general_purpose";
    const FILE_NAME: &str = "a.txt";

    let part_size = 1024 * 1024;
    let pool = PagedPool::new_with_candidate_sizes([part_size]);
    let client = Arc::new(
        MockClient::config()
            .bucket(BUCKET_NAME)
            .part_size(part_size)
            .enable_backpressure(true)
            .initial_read_window_size(256 * 1024)
            .enable_rename(false)
            .build(),
    );

    // Put one object into the bucket
    let params = PutObjectSingleParams::new();
    client
        .put_object_single(BUCKET_NAME, FILE_NAME, &params, "content")
        .await
        .expect("put object should have succeeded");

    let counter = client.new_counter(Operation::RenameObject);
    // Try to rename twice
    let fs = make_test_filesystem_with_client(
        client.clone(),
        pool,
        BUCKET_NAME,
        &Default::default(),
        Default::default(),
    );
    let err = fs
        .rename(
            FUSE_ROOT_INODE,
            FILE_NAME.as_ref(),
            FUSE_ROOT_INODE,
            "b.txt".as_ref(),
            RenameFlags::empty(),
        )
        .await
        .expect_err("rename should fail");
    assert_eq!(err.to_errno(), libc::ENOSYS, "rename should fail with ENOSYS");
    let err = fs
        .rename(
            FUSE_ROOT_INODE,
            FILE_NAME.as_ref(),
            FUSE_ROOT_INODE,
            "b.txt".as_ref(),
            RenameFlags::empty(),
        )
        .await
        .expect_err("rename should fail");
    assert_eq!(err.to_errno(), libc::ENOSYS, "rename should again fail with ENOSYS");
    assert_eq!(counter.count(), 1, "The second failed rename should have been cached");
}
