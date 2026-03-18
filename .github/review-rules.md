# Rules for AI Code Review

- Prefer explicit `SomeType::from(x)` over implicit `x.into()`.

- In new code, don't use `transmute_from_u8`, `transmute_to_u8`, `transmute_from_u8_to_slice`, `transmute_from_u8_to_mut_slice`, `transmute_to_u8_slice`.
  Use `bytemuck` or `zerocopy` crates instead.

- Prefer explicit exhaustive matches over catch-all `_` arm, so we don't miss handling new enum variants when they are added later.
  ```rust
  // good
  match my_enum {
      MyEnum::Foo(x) => handle_variant1(x),
      MyEnum::Bar(x, y) => handle_variant2(x, y),
      MyEnum::Baz(..) => {}
      MyEnum::Qux(..) => {}
  }

  // bad
  match my_enum {
      MyEnum::Variant1(x) => handle_variant1(x),
      MyEnum::Variant2(x, y) => handle_variant2(x, y),
      _ => {}
  }
  ```

  Exceptions:
  - Code in tests and benchmarks.
  - It's OK if you are sure that newly added enum variants will not affect the logic.

- Similarly, prefer explicit field ignoring using `: _` over using `..`.
  ```rust
  // good
  let MyStruct { a, b, c: _, d: _ } = my_struct;

  // bad
  let MyStruct { a, b, .. } = my_struct;
  ```

  Exceptions:
  - Code in tests and benchmarks.
  - It's OK if you are sure that newly added fields will not affect the logic.
    ```rust
    // OK (per the exception above)
    let status_code = match &my_error {
        MyError::BadInput { .. } => StatusCode::BAD_REQUEST,
        MyError::NotFound { .. } => StatusCode::NOT_FOUND,
        MyError::ServiceError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
    };
    ```

## Negative rules

Rules that are intended to reduce noise in AI code review and are not considered as issues.

- We don't mind using `.unwrap()` and `panic!()` in tests and benchmarks.

- For Rust, checking whether the code compiles is out of scope of AI code review. In particular:
  - Don't report mising imports for `size_of` or `align_of`.
    The prelude used in Rust 1.80+ includes these functions.
  - Don't report that some method is used but not implemented for a type. Most likely, it is.
  - Some versions of the `rand` crate provide methods named `random_*`, and some provide `gen_*`, e.g., `random_range` vs `gen_range`.
    Don't report that a "wrong" method is used, it's a compiler job.
  - Any other reports related to compiler errors or warnings.
