use diesel::mysql::data_types::MysqlTime;
use diesel::mysql::MysqlType;
use diesel::mysql::MysqlValue;
use mysql_async::{Params, Value};
use std::convert::TryInto;

pub(super) struct ToSqlHelper {
    pub(super) metadata: Vec<MysqlType>,
    pub(super) binds: Vec<Option<Vec<u8>>>,
}

fn to_value((metadata, bind): (MysqlType, Option<Vec<u8>>)) -> Value {
    match bind {
        Some(bind) => match metadata {
            MysqlType::Tiny => Value::Int((bind[0] as i8) as i64),
            MysqlType::Short => Value::Int(i16::from_ne_bytes(bind.try_into().unwrap()) as _),
            MysqlType::Long => Value::Int(i32::from_ne_bytes(bind.try_into().unwrap()) as _),
            MysqlType::LongLong => Value::Int(i64::from_ne_bytes(bind.try_into().unwrap())),

            MysqlType::UnsignedTiny => Value::UInt(bind[0] as _),
            MysqlType::UnsignedShort => {
                Value::UInt(u16::from_ne_bytes(bind.try_into().unwrap()) as _)
            }
            MysqlType::UnsignedLong => {
                Value::UInt(u32::from_ne_bytes(bind.try_into().unwrap()) as _)
            }
            MysqlType::UnsignedLongLong => {
                Value::UInt(u64::from_ne_bytes(bind.try_into().unwrap()))
            }
            MysqlType::Float => Value::Float(f32::from_ne_bytes(bind.try_into().unwrap())),
            MysqlType::Double => Value::Double(f64::from_ne_bytes(bind.try_into().unwrap())),

            MysqlType::Time => {
                let time: MysqlTime = diesel::deserialize::FromSql::<
                    diesel::sql_types::Time,
                    diesel::mysql::Mysql,
                >::from_sql(MysqlValue::new(&bind, metadata))
                .expect("This does not fail");
                Value::Time(
                    time.neg,
                    time.day as _,
                    time.hour as _,
                    time.minute as _,
                    time.second as _,
                    time.second_part as _,
                )
            }
            MysqlType::Date | MysqlType::DateTime | MysqlType::Timestamp => {
                let time: MysqlTime = diesel::deserialize::FromSql::<
                    diesel::sql_types::Timestamp,
                    diesel::mysql::Mysql,
                >::from_sql(MysqlValue::new(&bind, metadata))
                .expect("This does not fail");
                Value::Date(
                    time.year as _,
                    time.month as _,
                    time.day as _,
                    time.hour as _,
                    time.minute as _,
                    time.second as _,
                    time.second_part as _,
                )
            }
            MysqlType::Numeric
            | MysqlType::Set
            | MysqlType::Enum
            | MysqlType::String
            | MysqlType::Blob => Value::Bytes(bind),
            MysqlType::Bit => unimplemented!(),
            _ => unreachable!(),
        },
        None => Value::NULL,
    }
}

impl From<ToSqlHelper> for Params {
    fn from(ToSqlHelper { metadata, binds }: ToSqlHelper) -> Self {
        let values = metadata.into_iter().zip(binds).map(to_value).collect();
        Params::Positional(values)
    }
}
