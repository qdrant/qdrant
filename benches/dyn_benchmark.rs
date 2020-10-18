use criterion::{black_box, criterion_group, criterion_main, Criterion};

trait MyProcessor {
    fn inc(&mut self);
}

struct ImplMyProcessor {
    data: i64
}

impl MyProcessor for ImplMyProcessor {
    fn inc(&mut self) {
        self.data += 1;
    }
}

struct BackendA<'a> {
    data: Box<&'a mut dyn MyProcessor>
}

struct BackendB<'a, T: MyProcessor> {
    data: &'a mut T
}

impl<'a> BackendA<'a> {
    #[inline(never)]
    fn inc(&mut self, how_much: usize) {
        for _i in 0..how_much {
            self.inc_help()
        }
    }

    #[inline(never)]
    fn inc_help(&mut self) {
        self.data.inc();
    }
}

impl<'a, T: MyProcessor> BackendB<'a, T> {
    #[inline(never)]
    fn inc(&mut self, how_much: usize) {
        for _i in 0..how_much {
            self.inc_help()
        }
    }

    #[inline(never)]
    fn inc_help(&mut self) {
        self.data.inc();
    }
}


fn criterion_benchmark(c: &mut Criterion) {
  c.bench_function("Static dispatch", |b| b.iter(|| { 
    let mut d = ImplMyProcessor {data: 0};
    let mut b = BackendB::<ImplMyProcessor> {data: &mut d};
    for _i in 0..1 {
        b.inc(black_box(500_000));
    }
  }));

  c.bench_function("Dynamic dispatch", |b| b.iter(|| { 
    let mut d = ImplMyProcessor {data: 0};
    let mut b = BackendA {data: Box::new(&mut d)};
    for _i in 0..1 {
        b.inc(black_box(500_000));
    }
  }));
}


criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
