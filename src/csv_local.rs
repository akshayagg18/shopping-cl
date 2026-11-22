//! A line-local event scanner: complete the first three fields in one 64-byte
//! window, then start a second window at the ingestion field. No separator
//! arrays or newline bitmaps are materialized for these common-size rows.
use crate::{scan::{Flds,masks64},parse,memchr,file_bytes};

#[inline(always)]
fn through(bit:u32)->u64 {u64::MAX>>(63-bit)}

/// Each opening quote carries to its ending separator. The actual closing
/// quote must end at that same separator. Stray and single-byte quotes fail.
#[inline(always)]
fn quotes_ok(separators:u64,quotes:u64)->bool {
    let starts=(separators<<1)|1;
    let ends=separators>>1;
    let opening=quotes&starts;
    let expected=opening.wrapping_add(!separators)&separators;
    let actual=(quotes<<1)&separators;
    quotes&!(starts|ends)==0&&quotes&starts&ends==0&&expected==actual
}

#[inline]
fn fast(data:&[u8],p:usize)->Option<(usize,Flds<7>)> {
    if data.len()-p<128 {return None;}
    let (nl,cm,q)=masks64(&data[p..p+64]);
    if cm.count_ones()<3 {return None;}
    let mut remaining=cm;
    let c0=remaining.trailing_zeros();remaining&=remaining-1;
    let c1=remaining.trailing_zeros();remaining&=remaining-1;
    let c2=remaining.trailing_zeros();let mask=through(c2);
    if nl&mask!=0||!quotes_ok(cm&mask,q&mask) {return None;}
    let second=p+c2 as usize+1;
    let (nl,cm,q)=masks64(&data[second..second+64]);
    if nl==0 {return None;}
    let end=nl.trailing_zeros();let mask=through(end);let cm=cm&mask;
    if cm.count_ones()!=3||!quotes_ok((cm|nl)&mask,q&mask) {return None;}
    let mut remaining=cm;
    let c3=remaining.trailing_zeros();remaining&=remaining-1;
    let c4=remaining.trailing_zeros();remaining&=remaining-1;
    let c5=remaining.trailing_zeros();let base=c2+1;
    Some((second+end as usize,Flds{e:[c0,c1,c2,base+c3,base+c4,base+c5,base+end]}))
}

pub fn records(f:usize,s:usize,e:usize,each:impl FnMut(usize,&'static [u8],Option<&Flds<7>>)) {
    static ENABLED:std::sync::OnceLock<bool>=std::sync::OnceLock::new();
    if *ENABLED.get_or_init(||std::env::var("SS_LOCAL_CSV").is_ok_and(|v|v!="0")) {local_records(f,s,e,each);}
    else {crate::scan::scan_records::<7>(f,s,e,each);}
}

fn local_records(f:usize,s:usize,e:usize,mut each:impl FnMut(usize,&'static [u8],Option<&Flds<7>>)) {
    let data=file_bytes(f);let n=data.len();
    let mut p=if s==0 {memchr(b'\n',data).map_or(n,|i|i+1)}
        else if data[s-1]==b'\n' {s}
        else {memchr(b'\n',&data[s..]).map_or(n,|i|s+i+1)};
    while p<e {
        if let Some((end,fields))=fast(data,p) {each(p,&data[p..end],Some(&fields));p=end+1;continue;}
        let end=memchr(b'\n',&data[p..]).map_or(n,|i|p+i);
        let line=&data[p..end];let fields=parse::split::<7>(line).map(|f|Flds{e:f.ends});
        each(p,line,fields.as_ref());p=end+1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn scalar_quotes(line:&[u8])->bool {
        line.split(|&b|b==b',').all(|field| {
            if field.first()==Some(&b'"') {field.len()>=2&&field.last()==Some(&b'"')&&!field[1..field.len()-1].contains(&b'"')}
            else {!field.contains(&b'"')}
        })
    }
    #[test]
    fn carry_quote_check_matches_literal_dialect() {
        let alphabet=b"a,\"\r\0";
        for len in 0..=8 {
            for mut value in 0..5usize.pow(len) {
                let mut line=vec![0;len as usize];let mut sep=1u64<<len;let mut q=0;
                for (i,c) in line.iter_mut().enumerate() {*c=alphabet[value%5];value/=5;if *c==b',' {sep|=1<<i;}if *c==b'"' {q|=1<<i;}}
                assert_eq!(quotes_ok(sep,q),scalar_quotes(&line),"{:?}",line);
            }
        }
        let mut seed=17u64;
        for _ in 0..100000 {
            seed^=seed<<13;seed^=seed>>7;seed^=seed<<17;let n=(seed%64) as usize;
            let mut line=vec![0;n];let mut sep=1u64<<n;let mut q=0;
            for (i,c) in line.iter_mut().enumerate() {seed^=seed<<13;seed^=seed>>7;seed^=seed<<17;*c=alphabet[seed as usize%5];if *c==b','{sep|=1<<i;}if *c==b'"'{q|=1<<i;}}
            assert_eq!(quotes_ok(sep,q),scalar_quotes(&line));
        }
    }
    #[test]
    fn local_windows_match_scalar_with_chunks_and_long_fields() {
        let _guard=crate::tests_support::LOCK.lock().unwrap_or_else(|e|e.into_inner());
        let mut data=b"h,h,h,h,h,h,h\n".to_vec();let mut seed=13u64;
        for row in 0..5000 {
            for column in 0..7 {
                seed^=seed<<13;seed^=seed>>7;seed^=seed<<17;
                let quoted=seed&3!=0;if quoted {data.push(b'"');}
                let len=if row%333==0&&column==3 {10000} else {(seed%26) as usize};
                for _ in 0..len {data.push(b'a'+(seed%26) as u8);}
                if quoted {data.push(b'"');}
                if row%101==0&&column==2 {data.push(b'"');}
                if column!=6 {data.push(b',');}
            }
            data.push(b'\n');
        }
        data.pop();crate::tests_support::install(&data);
        let expected=crate::tests_support::naive::<7>(&data);
        for chunk in [1,31,64,127,192<<10] {
            let mut actual=Vec::new();
            for s in (0..data.len()).step_by(chunk) {
                local_records(0,s,(s+chunk).min(data.len()),|p,line,fields|actual.push((p,fields.map(|f|f.pairs(line)))));
            }
            if actual!=expected {
                let at=actual.iter().zip(&expected).position(|(a,b)|a!=b).unwrap_or(actual.len().min(expected.len()));
                panic!("chunk {chunk}: actual {:?}, expected {:?}, lengths {}/{}",actual.get(at),expected.get(at),actual.len(),expected.len());
            }
        }
    }
}
