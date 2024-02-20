USE fc_catalog;
CREATE COLUMNFAMILY comment (
    id varchar,
    textOriginal varchar,
    channelId varchar,
    videoId varchar,
    Sentiment varchar,
    PRIMARY KEY(id)
);