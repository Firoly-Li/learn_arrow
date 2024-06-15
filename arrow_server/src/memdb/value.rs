use super::Value;
use bytes::{BufMut, Bytes, BytesMut};
use parquet::data_type::AsBytes;
use std::fmt;

// // 定义一个枚举来表示可能的值类型
// #[derive(Debug)]
// pub enum Value {
//     Int(i32),
//     Float(f64),
//     String(String),
//     // 其他可能的值类型...
// }

// impl Value {
//     // 定义一个方法来获取实际的值
//     fn value<T>(&self) -> Option<&T>
//     where
//         T: 'static + fmt::Debug,
//     {
//         if self.as_any().type_id() == TypeId::of::<T>() {
//             self.as_any().downcast_ref::<T>()
//         } else {
//             None
//         }
//     }

//     // 添加一个方法来尝试获取内部值的引用，作为 Any 类型
//     fn as_any(&self) -> &dyn Any {
//         match self {
//             Value::Int(v) => v as &dyn Any,
//             Value::Float(v) => v as &dyn Any,
//             Value::String(v) => v as &dyn Any,
//             // 对于其他可能的值类型，添加相应的匹配分支
//             // ...
//         }
//     }
// }

// // 为Value实现Display trait，以便打印值
// impl fmt::Display for Value {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         match self {
//             Self::Int(v) => write!(f, "{}", v),
//             Self::Float(v) => write!(f, "{}", v),
//             Self::String(v) => write!(f, "{}", v),
//             // 其他可能的值类型...
//         }
//     }
// }

// #[cfg(test)]
// mod tests {
//     use crate::memdb::value::Value;

//     #[test]
//     fn test() {
//         let int_value = Value::Int(42);
//         let float_value = Value::Float(3.14);
//         let string_value = Value::String("Hello, world!".to_string());

//         println!("int_value: {}", int_value.value::<i32>().unwrap());
//         println!("float_value: {}", float_value.value::<f64>().unwrap());
//         println!("string_value: {}", string_value.value::<String>().unwrap());

//     }
// }

#[derive(Debug, Clone)]
pub enum VT {
    Int,
    Long,
    Float,
    Double,
    String,
    Bool,
    Bytes,
}

#[derive(Debug, Clone)]
pub struct Values {
    vt: VT,
    v: Bytes,
}

impl Values {
    fn int(v: i32) -> Self {
        Self {
            vt: VT::Int,
            v: Bytes::from(v.to_be_bytes().to_vec()),
        }
    }

    fn string(s: impl Into<String>) -> Self {
        Self {
            vt: VT::String,
            v: Bytes::from(s.into()),
        }
    }

    fn bool(b: bool) -> Self {
        let byte_slice = &[if b { 0x01 } else { 0x00 }];
        let bytes = Bytes::copy_from_slice(byte_slice);
        Self {
            vt: VT::Bool,
            v: bytes,
        }
    }
}

impl Value for Values {
    fn value<T: TryFrom<Bytes> + Clone + fmt::Debug>(&self) -> anyhow::Result<T> {
        match T::try_from(self.v.clone()) {
            Ok(v) => Ok(v),
            Err(_e) => Err(anyhow::Error::msg("message")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Values;

    #[test]
    fn test() {
        let s: Values = Values::int(5);
        println!("s: {:?}", s);
    }
}
