use crate::fs::DirectoryEntry;
use crate::fs::DirectoryReplier;
use crate::superblock::path::ValidKey;
use crate::superblock::InodeError;
use crate::superblock::InodeKind;
use crate::superblock::InodeNo;
use crate::superblock::InodeStat;
use crate::superblock::WriteMode;
use async_trait::async_trait;
use fuser::FileAttr;
use mountpoint_s3_client::types::ETag;
use std::ffi::OsStr;
use std::fmt::Debug;
use std::time::Duration;
use time::OffsetDateTime;
use tracing::debug;

pub trait AttibuteInformationProvider {
    fn kind(&self) -> InodeKind;
    fn stat(&self) -> &InodeStat;
    fn ino(&self) -> InodeNo;
    fn is_remote(&self) -> bool;
    fn validity(&self) -> Duration;
}

pub struct MetaBlockDirectoryEntryInformation {}

pub struct MountspaceDirectoryReplier<'a> {
    reply: &'a mut (dyn DirectoryReplier + Send + Sync),
    file_attr_creator: &'a (dyn Fn(&dyn AttibuteInformationProvider) -> FileAttr + Send + Sync),
}

impl<'a> MountspaceDirectoryReplier<'a> {
    pub fn new<R: DirectoryReplier + 'a + Send + Sync>(
        reply: &'a mut R,
        file_attr_creator: &'a (dyn Fn(&dyn AttibuteInformationProvider) -> FileAttr + Send + Sync),
    ) -> Self {
        MountspaceDirectoryReplier {
            reply,
            file_attr_creator,
        }
    }

    pub fn add(
        &mut self,
        file_attributes: &dyn AttibuteInformationProvider,
        name: &OsStr,
        offset: i64,
        generation: u64,
    ) -> bool {
        let attr = (self.file_attr_creator)(file_attributes);
        debug!("made attr");
        let result = self.reply.add(DirectoryEntry {
            ino: file_attributes.ino(),
            offset,
            name: name.to_os_string(),
            attr,
            generation,
            ttl: file_attributes.validity(),
        });
        debug!("{result} in mountspace");
        result
    }
}

#[derive(Clone, Debug)]
pub struct S3Location {
    pub bucket: String,
    pub full_key: ValidKey,
}

#[derive(Clone, Debug)]
pub struct LookedUp {
    pub ino: InodeNo,
    pub stat: InodeStat,
    pub kind: InodeKind,
    pub is_remote: bool,
    pub location: Option<S3Location>,
}

impl LookedUp {
    /// How much longer this lookup will be valid for
    pub fn validity(&self) -> Duration {
        self.stat.expiry.remaining_ttl()
    }

    pub fn s3_location(&self) -> Result<&S3Location, InodeError> {
        self.location.as_ref().ok_or(InodeError::VirtualFileNotAccessible)
    }
}

/// Two structures that can be used to communicate from Mountspace to Filesystem.
/// The Mountspace implementation MUST store the inode number corresponding to the read or writehandle
/// but MAY additionally store a unique number to identify this Read Handle.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WriteHandleNo(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReadHandleNo(pub u64);

#[derive(Debug)]
pub struct ReadHandle {
    pub no: Option<ReadHandleNo>,
    pub ino: InodeNo,
}

#[derive(Debug)]
pub struct WriteHandle {
    pub no: Option<WriteHandleNo>,
    pub ino: InodeNo,
}

#[async_trait]
pub trait Mountspace: Send + Sync + Debug {
    async fn lookup(&self, parent_ino: InodeNo, name: &OsStr) -> Result<LookedUp, InodeError>;

    async fn getattr(&self, ino: InodeNo, force_revalidate: bool) -> Result<LookedUp, InodeError>;

    async fn setattr(
        &self,
        ino: InodeNo,
        atime: Option<OffsetDateTime>,
        mtime: Option<OffsetDateTime>,
    ) -> Result<LookedUp, InodeError>;

    async fn create(&self, dir: InodeNo, name: &OsStr, kind: InodeKind) -> Result<LookedUp, InodeError>;

    async fn forget(&self, ino: InodeNo, n: u64);

    async fn start_writing(&self, ino: InodeNo, mode: &WriteMode, is_truncate: bool)
        -> Result<WriteHandle, InodeError>;

    async fn inc_file_size(&self, handle: &WriteHandle, len: usize) -> Result<usize, InodeError>;

    async fn finish_writing(&self, handle: &WriteHandle, etag: Option<ETag>) -> Result<(), InodeError>;

    async fn start_reading(&self, ino: InodeNo) -> Result<ReadHandle, InodeError>;

    async fn finish_reading(&self, handle: &ReadHandle) -> Result<(), InodeError>;

    async fn new_readdir_handle(&self, dir_ino: InodeNo, page_size: usize) -> Result<u64, InodeError>;

    async fn readdir<'a>(
        &self,
        parent: InodeNo,
        fh: u64,
        offset: i64,
        is_readdirplus: bool,
        reply: MountspaceDirectoryReplier<'a>,
    ) -> Result<(), InodeError>;

    async fn rename(
        &self,
        src_parent_ino: InodeNo,
        src_name: &OsStr,
        dst_parent_ino: InodeNo,
        dst_name: &OsStr,
        allow_overwrite: bool,
    ) -> Result<(), InodeError>;

    async fn rmdir(&self, parent_ino: InodeNo, name: &OsStr) -> Result<(), InodeError>;

    async fn unlink(&self, parent_ino: InodeNo, name: &OsStr) -> Result<(), InodeError>;
}
