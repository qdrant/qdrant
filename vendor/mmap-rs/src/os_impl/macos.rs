use crate::areas::{MemoryArea, Protection, ShareMode};
use crate::error::Error;
use libc::proc_regionfilename;
use mach2::{
    kern_return::{KERN_INVALID_ADDRESS, KERN_SUCCESS},
    port::mach_port_name_t,
    traps::{mach_task_self, task_for_pid},
    vm::mach_vm_region_recurse,
    vm_inherit::VM_INHERIT_SHARE,
    vm_prot::{VM_PROT_EXECUTE, VM_PROT_READ, VM_PROT_WRITE},
    vm_region::{vm_region_recurse_info_t, vm_region_submap_info_64},
    vm_types::mach_vm_address_t,
};
use nix::unistd::getpid;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::marker::PhantomData;
use std::ops::Range;
use std::path::Path;

#[derive(Debug)]
pub struct MemoryAreas<B> {
    pid: u32,
    task: mach_port_name_t,
    address: mach_vm_address_t,
    end: Option<usize>,
    marker: PhantomData<B>,
}

impl MemoryAreas<BufReader<File>> {
    pub fn open(pid: Option<u32>, range: Option<Range<usize>>) -> Result<Self, Error> {
        let task = unsafe { mach_task_self() };

        if let Some(pid) = pid {
            let result = unsafe { task_for_pid(task, pid as i32, &task as *const u32 as *mut u32) };

            if result != KERN_SUCCESS {
                return Err(Error::Mach(result));
            }
        }

        Ok(Self {
            pid: pid.unwrap_or(getpid().as_raw() as _),
            task,
            address: range
                .as_ref()
                .map(|range| range.start as mach_vm_address_t)
                .unwrap_or(0),
            end: range.map(|range| range.end),
            marker: PhantomData,
        })
    }
}

impl<B: BufRead> Iterator for MemoryAreas<B> {
    type Item = Result<MemoryArea, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut size = 0;
        let mut info: vm_region_submap_info_64 = unsafe { std::mem::zeroed() };
        let mut depth = 0;

        loop {
            let mut current_depth = depth;

            let result = unsafe {
                mach_vm_region_recurse(
                    self.task,
                    &mut self.address,
                    &mut size,
                    &mut current_depth,
                    (&mut info as *mut _) as vm_region_recurse_info_t,
                    &mut vm_region_submap_info_64::count(),
                )
            };

            match result {
                KERN_INVALID_ADDRESS => return None,
                KERN_SUCCESS => (),
                _ => return Some(Err(Error::Mach(result))),
            }

            if info.is_submap != 0 {
                depth += 1;
                continue;
            }

            let start = self.address as usize;
            let end = start + size as usize;
            let range = start..end;

            if let Some(end) = self.end {
                if start >= end {
                    return None;
                }
            }

            let mut protection = Protection::empty();

            if info.protection & VM_PROT_READ == VM_PROT_READ {
                protection |= Protection::READ;
            }

            if info.protection & VM_PROT_WRITE == VM_PROT_WRITE {
                protection |= Protection::WRITE;
            }

            if info.protection & VM_PROT_EXECUTE == VM_PROT_EXECUTE {
                protection |= Protection::EXECUTE;
            }

            let mut bytes = [0u8; libc::PATH_MAX as _];

            let result = unsafe {
                proc_regionfilename(
                    self.pid as _,
                    self.address,
                    bytes.as_mut_ptr() as _,
                    bytes.len() as _,
                )
            };

            let path = if result == 0 {
                None
            } else {
                let path = match std::str::from_utf8(&bytes[..result as usize]) {
                    Ok(path) => path,
                    Err(e) => return Some(Err(Error::Utf8(e))),
                };

                Some((Path::new(path).to_path_buf(), info.offset as u64))
            };

            let share_mode = match info.inheritance {
                VM_INHERIT_SHARE => ShareMode::Shared,
                _ => ShareMode::Private,
            };

            self.address = self.address.saturating_add(size);

            return Some(Ok(MemoryArea {
                allocation_base: range.start,
                range,
                protection,
                share_mode,
                path,
            }));
        }
    }
}
