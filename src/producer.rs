use crate::AZSync;

// #[derive(Debug)]
// enum ProducerTyp {
//     PUBLISH,
//     UNPUBLISH,
// }


struct Producer {
    id: usize,
}

impl AZSync for Producer {
    fn sync(&self) {
        println!("Producer {} is syncing", self.id);
    }
}