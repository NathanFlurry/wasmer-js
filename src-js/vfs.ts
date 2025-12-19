/**
 * VFS (Virtual File System) API for wasmer-js instances.
 *
 * This module provides a convenient wrapper around the raw VFS methods
 * exposed on Instance, offering a cleaner interface for filesystem operations.
 */

// Import the Instance type from the generated WASM bindings
// Note: This is imported from the generated pkg directory
import type { Instance } from "../pkg/wasmer_js";

/**
 * Metadata about a file or directory in the VFS.
 */
export interface VfsStat {
  /** Is this path a file? */
  isFile: boolean;
  /** Is this path a directory? */
  isDir: boolean;
  /** Size of the file in bytes (0 for directories). */
  size: number;
}

/**
 * An entry in a directory listing.
 */
export interface VfsDirEntry {
  /** The name of the file or directory. */
  name: string;
  /** The full path to the entry. */
  path: string;
}

/**
 * Virtual File System interface for accessing files within a WASIX instance.
 */
export interface VFS {
  /**
   * Read the contents of a file as binary data.
   * @param path - Absolute path in the WASIX filesystem
   * @returns File contents as Uint8Array
   */
  readFile(path: string): Promise<Uint8Array>;

  /**
   * Read the contents of a file as UTF-8 text.
   * @param path - Absolute path in the WASIX filesystem
   * @returns File contents as string
   */
  readTextFile(path: string): Promise<string>;

  /**
   * Write data to a file, creating it if it doesn't exist, or truncating if it does.
   * Parent directories are created automatically.
   * @param path - Absolute path in the WASIX filesystem
   * @param content - Data to write (string or Uint8Array)
   */
  writeFile(path: string, content: Uint8Array | string): Promise<void>;

  /**
   * Check if a path exists (file or directory).
   * @param path - Absolute path to check
   * @returns true if path exists, false otherwise
   */
  exists(path: string): boolean;

  /**
   * Get metadata about a file or directory.
   * @param path - Absolute path to stat
   * @returns VfsStat object with metadata
   */
  stat(path: string): VfsStat;

  /**
   * List contents of a directory.
   * @param path - Absolute path to directory
   * @returns Array of directory entries
   */
  readDir(path: string): VfsDirEntry[];

  /**
   * Create a directory and all parent directories.
   * @param path - Absolute path of directory to create
   */
  mkdir(path: string): void;

  /**
   * Remove a file.
   * @param path - Absolute path of file to remove
   */
  removeFile(path: string): void;

  /**
   * Remove an empty directory.
   * @param path - Absolute path of directory to remove
   */
  removeDir(path: string): void;
}

/**
 * Create a VFS wrapper around a wasmer-js Instance.
 *
 * @param instance - The Instance to wrap
 * @returns A VFS object providing filesystem operations
 *
 * @example
 * ```typescript
 * const instance = await Wasmer.runWasix(wasm, { args: ['run'] });
 * const vfs = createVFS(instance);
 *
 * // Write a file
 * await vfs.writeFile('/app/config.json', JSON.stringify({ debug: true }));
 *
 * // Read a file
 * const content = await vfs.readTextFile('/app/config.json');
 * ```
 */
export function createVFS(instance: Instance): VFS {
  return {
    readFile(path: string): Promise<Uint8Array> {
      return instance.vfsReadFile(path);
    },

    readTextFile(path: string): Promise<string> {
      return instance.vfsReadTextFile(path);
    },

    async writeFile(path: string, content: Uint8Array | string): Promise<void> {
      if (typeof content === "string") {
        return instance.vfsWriteTextFile(path, content);
      }
      return instance.vfsWriteFile(path, content);
    },

    exists(path: string): boolean {
      return instance.vfsExists(path);
    },

    stat(path: string): VfsStat {
      return instance.vfsStat(path) as VfsStat;
    },

    readDir(path: string): VfsDirEntry[] {
      return Array.from(instance.vfsReadDir(path)) as VfsDirEntry[];
    },

    mkdir(path: string): void {
      instance.vfsMkdir(path);
    },

    removeFile(path: string): void {
      instance.vfsRemoveFile(path);
    },

    removeDir(path: string): void {
      instance.vfsRemoveDir(path);
    },
  };
}
