use super::{Deserialize, Serialize};
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug)]
pub struct VarIntSigned(pub i64);
#[derive(Debug)]
pub struct VarIntUnsigned(pub u64);

impl Deserialize for VarIntUnsigned {
    fn deserialize(bytes: &mut Bytes) -> Self {
        let mut value = 0;
        let mut shift = 0;
        loop {
            let byte = bytes.get_u8();
            value |= ((byte & 0x7F) as u64) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        Self(value)
    }
}

impl Deserialize for VarIntSigned {
    fn deserialize(bytes: &mut Bytes) -> Self {
        // deserialize unsigned + zigzag decode
        let n = VarIntUnsigned::deserialize(bytes).0;
        Self(((n >> 1) as i64) ^ (-((n & 1) as i64)))
    }
}

impl Serialize for VarIntUnsigned {
    fn serialize(&self, bytes: &mut BytesMut) {
        let mut value = self.0;
        while value > 0x7F {
            bytes.put_u8((value & 0x7F) as u8 | 0x80);
            value >>= 7;
        }
        bytes.put_u8(value as u8);
    }
}
