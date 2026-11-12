use forest_onebot::{
    cq,
    message::{Message, Node, Segment},
};

#[test]
fn cq_escaping_preserves_literal_codes_parameters_and_nested_nodes() {
    let input = "&#91;CQ:at,qq=all&#93;&amp;#91;[CQ:share,title=含=等号&#44;逗号,url=https://example.com/?a=1&amp;b=2][CQ:node,user_id=7,nickname=转发者,content=&#91;CQ:face&#44;id=123&#93;哈喽～]";
    let parts = cq::decode(input).unwrap();
    assert_eq!(parts[0], Segment::text("[CQ:at,qq=all]&#91;"));
    assert_eq!(
        parts[1],
        Segment::Share {
            url: "https://example.com/?a=1&b=2".into(),
            title: "含=等号,逗号".into(),
            content: None,
            image: None,
        }
    );
    let Segment::Node(Node::Custom { content, .. }) = &parts[2] else {
        panic!("node")
    };
    assert_eq!(
        cq::segments(content).unwrap().as_ref(),
        &[Segment::Face { id: "123".into() }, Segment::text("哈喽～"),]
    );
    assert_eq!(
        cq::encode(&Message::Segments(parts)).unwrap(),
        input.replace(
            "title=含=等号&#44;逗号,url=https://example.com/?a=1&amp;b=2",
            "url=https://example.com/?a=1&amp;b=2,title=含=等号&#44;逗号"
        )
    );
    assert_eq!(
        cq::decode("[CQ:image,file=x,file=y]"),
        Err(cq::Error::InvalidParameter)
    );
    assert_eq!(cq::decode("[CQ:image,file=x"), Err(cq::Error::Unterminated));
    assert_eq!(
        cq::decode("[CQ:future,x=y]"),
        Err(cq::Error::UnknownType("future".into()))
    );
}
