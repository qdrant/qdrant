
pub trait Persistable {
  pub fn persist(&self, directory: &Path);
  pub fn load(directory: &Path) -> Self;
}


/// Interface for vector storage
///  El - type of vector element, expected numbericl type
///  Id - type of the key
pub trait VectorStorage<El, K> {
  pub fn get_vector(&self, key: K) -> &Vec<El>;
  pub fn put_vector(&mut self, vector: &Vec<El>, key: K); 
}

pub enum AssosiatedData {
  Label {

  },
  
} 

trait PayloadStorage<K> {
  pub fn assign(&self, key: K,  )
}

trait Index {

}
