

pub static CREATE_TOPIC_STMT: &str = r"
    INSERT INTO topics (id, name, data) VALUES (?1, ?2, ?3)
";
pub static READ_TOPIC_STMT: &str = r"
    SELECT id, name, data FROM topics WHERE id = ?1;
";
pub static DELETE_TOPIC_STMT: &str = r"
    DELETE FROM topics WHERE name = ?1
";
pub static CREATE_CONSUMER_STMT: &str = r"
    INSERT INTO consumers (id) VALUES (?1)
";
pub static SUBSCRIBE_CONSUMER_STMT: &str = r"
    INSERT INTO consumer_to_topic (consumer_id, topic_id, offset) VALUES (?1, ?2, ?3)
";
pub static GET_OFFSET_CONSUMER_STMT: &str = r"
    SELECT consumer_id, topic_id, offset FROM consumer_to_topic WHERE (consumer_id, topic_id) = (?1, ?2)
";
pub static UPDATE_OFFSET_CONSUMER_STMT: &str = r"
    UPDATE consumer_to_topic SET offset = ?1 WHERE (consumer_id, topic_id) = (?2, ?3)
";