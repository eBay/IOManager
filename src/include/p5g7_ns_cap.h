#include <iostream>

struct spdk_nvme_ns_data {
    // namespace size: This field indicates the total size of the namespace in logical blocks. A namespace of size n
    // consists of LBA 0 through (n - 1). The number of logical blocks is based on the formatted LBA size. This field is
    // undefined prior to the namespace being formatted
    uint64_t nsze = 7501476528;

    // Namespace Capacity (NCAP): This field indicates the maximum number of logical blocks that may be allocated in the
    // namespace at any point in time. The number of logical blocks is based on the formatted LBA size. This field is
    // undefined prior to the namespace being formatted. This field is used in the case of thin provisioning and reports
    // a value that is smaller than or equal to the Namespace Size. Spare LBAs are not reported as part of this field. A
    // logical block is allocated when it is written with a Write or Write Uncorrectable command. A logical block may be
    // deallocated using the Dataset Management, Sanitize, or Write Zeroes command
    uint64_t ncap = 7501476528;

    // Namespace Utilization (NUSE): This field indicates the current number of logical blocks allocated in the
    // namespace. This field is smaller than or equal to the Namespace Capacity. The number of logical blocks is based
    // on the formatted LBA size.
    uint64_t nuse = 1075591328;

    // Namespace Features (NSFEAT): This field defines features of the namespace. Bits 7:5 are reserved.
    // Bit 4 if set to  ‘1’: indicates that the fields NPWG, NPWA, NPDG, NPDA, and NOWS are defined for this namespace
    // and should be used by the host for I/O optimization; and NOWS defined for this namespace shall adhere to Optimal
    // Write Size field setting defined in NVM Sets Attributes Entry (refer to Figure 251) for the NVM Set with which
    // this namespace is associated. If cleared to ‘0’, then:
    // • the controller does not support the fields NPWG, NPWA, NPDG, NPDA, and NOWS for this namespace; and
    // • Optimal Write Size field in NVM Sets Attributes Entry (refer to Figure 251) for the NVM Set with which this
    // namespace is associated should be used by the host for I/O optimization.
    //
    // Bit 3 if set to ‘1’ indicates that the value in the NGUID field for this namespace, if nonzero, is never reused
    // by the controller and that the value in the EUI64 field for this namespace, if non-zero, is never reused by the
    // controller. If cleared to ‘0’, then the NGUID value may be reused and the EUI64 value may be reused by the
    // controller for a new namespace created after this namespace is deleted. This bit shall be cleared to ‘0’ if both
    // NGUID and EUI64 fields are cleared to 0h.
    //
    // Bit 2 if set to ‘1’ indicates that the controller supports the Deallocated or Unwritten Logical Block error for
    // this namespace. If cleared to ‘0’, then the controller does not support the Deallocated or Unwritten Logical
    // Block error for this namespace.
    //
    // Bit 1 if set to ‘1’ indicates that the fields NAWUN, NAWUPF, and NACWU are defined for this namespace and should
    // be used by the host for this namespace instead of the AWUN, AWUPF, and ACWU fields in the Identify Controller
    // data structure. If cleared to ‘0’, then the controller does not support the fields NAWUN, NAWUPF, and NACWU for
    // this namespace.
    //
    // Bit 0 if set to ‘1’ indicates that the namespace supports thin provisioning. Specifically, the Namespace Capacity
    // reported may be less than the Namespace Size. When this feature is supported and the Dataset Management command
    // is supported, then deallocating LBAs shall be reflected in the Namespace Utilization field. Bit 0 if cleared to
    // ‘0’ indicates that thin provisioning is not supported
    struct nsfeat {
        uint8_t thin_prov = 0;                  /** thin provisioning */
        uint8_t ns_atomic_write_unit = 1;       /** NAWUN, NAWUPF, and NACWU are defined for this namespace */
        uint8_t dealloc_or_unwritten_error = 0; /** Supports Deallocated or Unwritten LBA error for this namespace */
        uint8_t guid_never_reused = 0;          /** Non-zero NGUID and EUI64 for namespace are never reused */
        uint8_t reserved1 = 0;
    };

    // Number of LBA Formats (NLBAF): This field defines the number of supported LBA data size and metadata size
    // combinations supported by the namespace. LBA formats shall be allocated in order (starting with 0) and packed
    // sequentially. This is a 0’s based value. The maximum number of LBA formats that may be indicated as supported
    // is 16. The supported LBA formats are indicated in bytes 128 to 191 in this data structure. The LBA Format fields
    // with an index beyond the value set in this field are invalid and not supported. LBA Formats that are valid, but
    // not currently available may be indicated by setting the LBA Data Size for that LBA Format to 0h. The metadata may
    // be either transferred as part of the LBA (creating an extended LBA which is a larger LBA size that is exposed to
    // the application) or may be transferred as a separate contiguous buffer of data. The metadata shall not be split
    // between the LBA and a separate metadata buffer. It is recommended that software and controllers transition to an
    // LBA size that is 4 KiB or larger for ECC efficiency at the controller. If providing metadata, it is recommended
    // that at least 8 bytes are provided per logical block to enable use with end-to-end data protection, refer to
    // section 8.2.
    uint8_t nlbaf = 1;

    // Formatted LBA Size (FLBAS): This field indicates the LBA data size & metadata size combination that the namespace
    // has been formatted with (refer to section 5.23).
    // Bits 7:5 are reserved.
    // Bit 4 if set to ‘1’ indicates that the metadata is transferred at the end of the data LBA, creating an extended
    // data LBA. Bit 4 if cleared to ‘0’ indicates that all of the metadata for a command is transferred as a separate
    // contiguous buffer of data. Bit 4 is not applicable when there is no metadata.
    // Bits 3:0 indicates one of the 16 supported LBA Formats indicated in this data structure.
    struct flbas {
        uint8_t format = 0;
        uint8_t extended = 0;
        uint8_t reserved2 = 0;
    };

    // Metadata Capabilities (MC): This field indicates the capabilities for metadata.
    // Bits 7:2 are reserved.
    // Bit 1 if set to ‘1’ indicates the namespace supports the metadata being transferred as part of a separate buffer
    // that is specified in the Metadata Pointer.
    // Bit 0 if set to ‘1’ indicates that the namespace supports the metadata being transferred as part of an extended
    // data LBA.
    struct mc {
        uint8_t extended = 0; /** metadata can be transferred as part of data prp list */
        uint8_t pointer = 0;  /** metadata can be transferred with separate metadata pointer */
        uint8_t reserved3 = 0;
    };

    // End-to-end Data Protection Capabilities (DPC): This field indicates the capabilities for the end-to-end data
    // protection feature. Multiple bits may be set in this field.
    // Bits 7:5 are reserved.
    // Bit 4 if set to ‘1’ indicates that the namespace supports protection information
    // transferred as the last eight bytes of metadata.
    //
    // Bit 3 if set to ‘1’ indicates that the namespace supports protection information transferred as the first eight
    // bytes of metadata.
    //
    // Bit 2,1, 0 if set to ‘1’ indicates that the namespace supports Protection Information Type 3,2,1 respectively.
    struct dpc {
        uint8_t pit1 = 0;     /** protection information type 1 */
        uint8_t pit2 = 0;     /** protection information type 2 */
        uint8_t pit3 = 0;     /** protection information type 3 */
        uint8_t md_start = 0; /** first eight bytes of metadata */
        uint8_t md_end = 0;   /** last eight bytes of metadata */
    };

    /** end-to-end data protection type settings */
    struct dps {
        uint8_t pit = 0; /** protection information type */

        /** 1 == protection info transferred at start of metadata */
        /** 0 == protection info transferred at end of metadata */
        uint8_t md_start = 0;

        uint8_t reserved4 = 0;
    };

    /** namespace multi-path I/O and namespace sharing capabilities */
    struct nmic {
        uint8_t can_share = 0;
        uint8_t reserved = 0;
    };

    /** reservation capabilities */
    union nsrescap {
        struct rescap {
            uint8_t persist = 0;                   /** supports persist through power loss */
            uint8_t write_exclusive = 0;           /** supports write exclusive */
            uint8_t exclusive_access = 0;          /** supports exclusive access */
            uint8_t write_exclusive_reg_only = 0;  /** supports write exclusive - registrants only */
            uint8_t exclusive_access_reg_only = 0; /** supports exclusive access - registrants only */
            uint8_t write_exclusive_all_reg = 0;   /** supports write exclusive - all registrants */
            uint8_t exclusive_access_all_reg = 0;  /** supports exclusive access - all registrants */
            uint8_t ignore_existing_key = 0;       /** supports ignore existing key */
        };
        uint8_t raw = 0;
    };

    /** format progress indicator */
    struct fpi {
        uint8_t percentage_remaining = 0;
        uint8_t fpi_supported = 1;
    };

    /** deallocate logical features */
    union dlfeat {
        uint8_t raw = 0;
        struct bits {
            /**
             * Value read from deallocated blocks
             *
             * 000b = not reported
             * 001b = all bytes 0x00
             * 010b = all bytes 0xFF
             *
             * \ref spdk_nvme_dealloc_logical_block_read_value
             */
            uint8_t read_value = 0;

            /** Supports Deallocate bit in Write Zeroes */
            uint8_t write_zero_deallocate = 0;

            /**
             * Guard field behavior for deallocated logical blocks
             * 0: contains 0xFFFF
             * 1: contains CRC for read value
             */
            uint8_t guard_value = 0;

            uint8_t reserved = 0;
        }
    };

    // Namespace Atomic Write Unit Normal (NAWUN): This field indicates the namespace specific size of the write
    // operation guaranteed to be written atomically to the NVM during normal operation. A value of 0h indicates that
    // the size for this namespace is the same size as that reported in the AWUN field of the Identify Controller data
    // structure. All other values specify a size in terms of logical blocks using the same encoding as the AWUN field.
    uint16_t nawun = 1023;

    // Namespace Atomic Write Unit Power Fail (NAWUPF): This field indicates the namespace specific size of the write
    // operation guaranteed to be written atomically to the NVM during a power fail or error condition.  A value of 0h
    // indicates that the size for this namespace is the same size as that reported in the AWUPF field of the Identify
    // Controller data structure. All other values specify a size in terms of logical blocks using the same encoding as
    // the AWUPF field
    uint16_t nawupf = 7;

    // Namespace Atomic Compare & Write Unit (NACWU): This field indicates the namespace specific size of the write
    // operation guaranteed to be written atomically to the NVM for a Compare and Write fused command.  A value of 0h
    // indicates that the size for this namespace is the same size as that reported in the ACWU field of the Identify
    // Controller data structure. All other values specify a size in terms of logical blocks using the same encoding as
    // the ACWU field
    uint16_t nacwu = 0;

    // Namespace Atomic Boundary Size Normal (NABSN): This field indicates the atomic boundary size for this namespace
    // for the NAWUN value. This field is specified in logical blocks. Writes to this namespace that cross atomic
    // boundaries are not guaranteed to be atomic to the NVM with respect to other read or write commands.  A value of
    // 0h indicates that there are no atomic boundaries for normal write operations.  All other values specify a size in
    // terms of logical blocks using the same encoding as the AWUN field.
    uint16_t nabsn = 1023;

    // Namespace Atomic Boundary Offset (NABO): This field indicates the LBA on this namespace where the first atomic
    // boundary starts. If the NABSN and NABSPF fields are cleared to 0h, then the NABO field shall be cleared to 0h.
    // NABO shall be less than or equal to NABSN and NABSPF.
    uint16_t nabo = 0;

    // Namespace Atomic Boundary Size Power Fail (NABSPF): This field indicates the atomic boundary size for this
    // namespace specific to the Namespace Atomic Write Unit Power Fail value. This field is specified in logical
    // blocks. Writes to this namespace that cross atomic boundaries are not guaranteed to be atomic with respect to
    // other read or write commands and there is no guarantee of data returned on subsequent reads of the associated
    // logical blocks. A value of 0h indicates that there are no atomic boundaries for power fail or error conditions.
    // All other values specify a size in terms of logical blocks using the same encoding as the AWUPF field.
    uint16_t nabspf = 7;

    // Namespace Optimal I/O Boundary (NOIOB): This field indicates the optimal I/O boundary for this namespace. This
    // field is specified in logical blocks. The host should construct Read and Write commands that do not cross the I/O
    // boundary to achieve optimal performance. A value of 0h indicates that no optimal I/O boundary is reported.
    // Refer to section 8.25 for how this field is utilized to improve performance and endurance.
    uint16_t noiob = 0;

    // NVM Capacity (NVMCAP): This field indicates the total size of the NVM allocated to this namespace. The value is
    // in bytes. This field shall be supported if the Namespace Management capability (refer to section 8.12) is
    // supported. Note: This field may not correspond to the logical block size multiplied by the Namespace Size field.
    // Due to thin provisioning or other settings (e.g., endurance), this field may be larger or smaller than the
    // Namespace Size reported. If the controller supports Asymmetric Namespace Access Reporting (refer to the CMIC
    // field), and the relationship between the controller and the namespace is in the ANA Inaccessible state (refer to
    // section 8.20.3.3) or the ANA Persistent Loss state (refer to section 8.20.3.4), then this field shall be cleared
    // to 0h.
    uint64_t nvmcap[2] = {3840755982336, 0}; /** NVM capacity */

    uint8_t reserved64[40];

    uint8_t nguid[16] = "4380M\201 &\224\000 % 8A\000\000\000\001";
    uint64_t eui64 = 0;

    struct lbaf {
        /** metadata size */
        uint32_t ms : 16;

        /** lba data size */
        uint32_t lbads : 8;

        /** relative performance */
        uint32_t rp : 2;

        uint32_t reserved6 : 6;
    };

    struct lbaf lba[16] = {
        {0, 9, 0, 0}, {0, 12, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0},
        {0, 0, 0, 0}, {0, 0, 0, 0},  {0, 0, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0},
    };

    uint8_t reserved6;
    uint8_t vendor_specific[3712] = "\000 < repeats 3711 times >";
};