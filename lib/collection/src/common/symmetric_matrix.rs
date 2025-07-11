/// Memory efficient and performant symmetric matrix implementation.
#[derive(Clone)]
pub struct SymmetricMatrix<I> {
    // Stores only the upper part matrix, flattened.
    inner: Vec<I>,

    /// Size of matrix. Size of 3 means 3 rows and 3 columns.
    size: usize,
}

impl<T: Copy> SymmetricMatrix<T> {
    /// Creates a new symmetric matrix with `init` as initial value.
    /// Returns `None` if `size` is smaller than 2 because this is the minimum size of a matrix.
    pub fn new(size: usize, init: T) -> Option<Self> {
        if size < 2 {
            return None;
        }

        Some(Self {
            inner: vec![init; Self::triangular(size)],
            size,
        })
    }
}

impl<T> SymmetricMatrix<T> {
    /// Set the value at row x column. Since it's a symmetric matrix, column and row can be swapped.
    #[inline]
    pub fn set(&mut self, row: usize, column: usize, value: T) {
        let (row, column) = Self::handle_index(row, column);
        let index = self.calculate_index(row, column);
        self.inner[index] = value;
    }

    /// Sets the value at row x column. Since it's a symmetric matrix, column and row can be swapped.
    #[inline]
    pub fn get(&self, row: usize, column: usize) -> &T {
        let (row, column) = Self::handle_index(row, column);
        let index = self.calculate_index(row, column);
        &self.inner[index]
    }

    /// Swap input parameters to always target the upper symmetric.
    #[inline]
    fn handle_index(row: usize, column: usize) -> (usize, usize) {
        if column < row {
            (column, row)
        } else {
            (row, column)
        }
    }

    /// Calculates the flattened index given the row and column. Parameters aren't allowed to be swapped!
    #[inline]
    fn calculate_index(&self, row: usize, column: usize) -> usize {
        // Prevent an out of bounds column to access elements from the next row.
        // Disabled in release for performance.
        debug_assert!(
            column < self.size,
            "Column index {column} out of bounds for {}x{} matrix",
            self.size,
            self.size
        );

        ((row * self.size) + column) - Self::triangular(row)
    }

    /// Triangular function: https://en.wikipedia.org/wiki/Triangular_number
    #[inline]
    fn triangular(x: usize) -> usize {
        (x * (x + 1)) / 2
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_matrix() {
        let size = 10;
        let mut matrix = SymmetricMatrix::<usize>::new(size, 0).unwrap();

        let mut c = 0;
        for x in 0..size {
            for y in x..size {
                matrix.set(x, y, c);
                c += 1;
            }
        }

        c = 0;

        for x in 0..size {
            for y in x..size {
                assert_eq!(*matrix.get(x, y), c);
                assert_eq!(*matrix.get(y, x), c);
                c += 1;
            }
        }
    }

    #[test]
    #[should_panic]
    fn test_oob() {
        let size = 10;
        let mut matrix = SymmetricMatrix::<usize>::new(size, 0).unwrap();
        matrix.set(0, 10, 1);
    }

    #[test]
    fn test_matrix_vs_naive_implementation() {
        let size = 5;
        let init_value = 42;

        // Create symmetric matrix
        let mut symmetric_matrix = SymmetricMatrix::<i32>::new(size, init_value).unwrap();

        // Create naive 2D vector matrix
        let mut naive_matrix = vec![vec![init_value; size]; size];

        // Set some values in both matrices
        let test_values = vec![
            (0, 1, 10),
            (1, 3, 20),
            (2, 4, 30),
            (0, 4, 40),
            (1, 2, 50),
            (3, 3, 100), // Also supports the diagonal elements
        ];

        for (row, col, value) in &test_values {
            symmetric_matrix.set(*row, *col, *value);
            // Insert into naive matrix
            naive_matrix[*row][*col] = *value;
            naive_matrix[*col][*row] = *value;
        }

        // Compare all values
        #[allow(clippy::needless_range_loop)]
        for row in 0..size {
            for col in 0..size {
                assert_eq!(
                    *symmetric_matrix.get(row, col),
                    naive_matrix[row][col],
                    "Mismatch at position ({row}, {col})",
                );
            }
        }
    }
}
