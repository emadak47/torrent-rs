use async_wss::{
    spsc::{QueueError, SPSCQueue},
    utils::FlatbufferEvent,
};

#[test]
fn test_send_receive_primitive() {
    let (mut tx, mut rx) = SPSCQueue::new::<u8>(20);
    let data = [2, 1, 4, 3, 6, 5, 10, 7, 8, 9];
    for i in 0..=4 {
        tx.push(data[i]);
    }
    assert_eq!(Ok(2), rx.pop());
    assert_eq!(Ok(1), rx.pop());
    assert_eq!(Ok(4), rx.pop());

    tx.push(data[5]);
    assert_eq!(Ok(3), rx.pop());
    assert_eq!(Ok(6), rx.pop());
    assert_eq!(Ok(5), rx.pop());
    assert_eq!(Err(QueueError::EmptyQueue), rx.pop());

    for i in 6..10 {
        tx.push(data[i]);
    }
    assert_eq!(Ok(10), rx.pop());
    assert_eq!(Ok(7), rx.pop());
    assert_eq!(Ok(8), rx.pop());
    assert_eq!(Ok(9), rx.pop());
    assert_eq!(Err(QueueError::EmptyQueue), rx.pop());
}

#[test]
fn test_send_receive_string() {
    let (mut tx, mut rx) = SPSCQueue::new::<String>(20);
    let data = ["1", "2", "#", "$"].map(String::from);
    for i in 0..2 {
        tx.push(data[i].to_owned());
    }
    assert_eq!(Ok("1".to_string()), rx.pop());
    assert_eq!(Ok("2".to_string()), rx.pop());
    assert_eq!(Err(QueueError::EmptyQueue), rx.pop());

    tx.push(data[2].to_owned());
    assert_eq!(Ok("#".to_string()), rx.pop());
    assert_eq!(Err(QueueError::EmptyQueue), rx.pop());

    tx.push(data[3].to_owned());
    assert_eq!(Ok("$".to_string()), rx.pop());

    assert_eq!(Err(QueueError::EmptyQueue), rx.pop());
}

#[test]
fn test_send_receive_flatbuffer() {
    let (mut tx, mut rx) = SPSCQueue::new::<FlatbufferEvent>(20);
    for i in 0..2 {
        tx.push(FlatbufferEvent {
            stream_id: i,
            buff: vec![1 * i, 2 * i, 3 * i],
        });
    }

    assert!(rx
        .pop()
        .is_ok_and(|v| v.stream_id == 0 && v.buff == vec![0, 0, 0]));

    assert!(rx
        .pop()
        .is_ok_and(|v| v.stream_id == 1 && v.buff == vec![1, 2, 3]));

    assert!(rx.pop().is_err_and(|e| e == QueueError::EmptyQueue));

    tx.push(FlatbufferEvent {
        stream_id: 2,
        buff: vec![11, 22, 33],
    });

    assert!(rx
        .pop()
        .is_ok_and(|v| v.stream_id == 2 && v.buff == vec![11, 22, 33]));
}
