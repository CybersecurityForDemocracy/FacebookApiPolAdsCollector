"""Schema adapted from https://github.com/CrowdTangle/API/wiki/Post. Kept in case we decide to write
Crowdtangle data to Big Query in the future."""
CROWDTANGLE_BIGQUERY_SCHEMAS = {
    'accounts' : {
            "description": "See account https://github.com/CrowdTangle/API/wiki/Account",
            "name": "accounts",
            "type": "RECORD",
            "mode": "REQUIRED",
            "fields": [
                {
                    "description": "The unique identifier of the account in the CrowdTangle system. This ID is specific to CrowdTangle, not the platform on which the account exists.",
                    "name": "id",
                    "type": "INT64",
                    "mode": "PRIMARY_KEY"
                },
                {
                    "description": "For Facebook only. Options are facebook_page, facebook_profile, facebook_group.",
                    "name": "accountType",
                    "type": "STRING",
                    "mode": "REQUIRED"
                },
                {
                    "description": "The handle or vanity URL of the account.",
                    "name": "handle",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "description": "The name of the account.",
                    "name": "name",
                    "type": "STRING",
                    "mode": "REQUIRED"
                },
                {
                    "description": "The ISO country code of the the country from where the plurality of page administrators operate.",
                    "name": "pageAdminTopCountry",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "description": "The platform on which the account exists. enum (facebook, instagram, reddit)",
                    "name": "platform",
                    "type": "STRING",
                    "mode": "REQUIRED"
                },
                {
                    "description": "The platform's ID for the account. This is not shown for Facebook public users.",
                    "name": "platformId",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "description": "A URL pointing at the profile image.",
                    "name": "profileImage",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "description": "The number of subscribers/likes/followers the account has. By default, the subscriberCount property will show page Followers (as of January 26, 2021). You can select either Page Likes or Followers in your Dashboard settings. https://help.crowdtangle.com/en/articles/4797890-faq-measuring-followers.",
                    "name": "subscriberCount",
                    "type": "INT64",
                    "mode": "NULLABLE"
                },
                {
                    "description": "A link to the account on its platform.",
                    "name": "url",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "description": "Whether or not the account is verified by the platform, if supported by the platform. If not supported, will return false.",
                    "name": "verified",
                    "type": "BOOLEAN",
                    "mode": "REQUIRED"
                }
            ]
        },
    'posts': {
        "description": "A post object represents a single post from any of the supported platforms (e.g., Facebook, Instagram).",
        "name": "posts",
        "type": "RECORD",
        "mode": "REQUIRED",
        "fields": [
        {
            "description": "format (\"account.id|postExternalId\")   The unique identifier of the post in the CrowdTangle system. This ID is specific to CrowdTangle, not the platform from which the post originated.",
            "name": "id",
            "type": "STRING",
            "mode": "PRIMARY_KEY"
        },
        {
            "description": "See account https://github.com/CrowdTangle/API/wiki/Account",
            "name": "account_id",
            "type": "INT64",
            "mode": "REQUIRED",
        },
        {
            "description": "See account https://github.com/CrowdTangle/API/wiki/Account . This field is only present for Facebook Page posts where there is a sponsoring Page.",
            "name": "brandedContentSponsor_account_id",
            "type": "INT64",
            "mode": "NULLABLE",
        },
        {
            "description": "The user-submitted text on a post.",
            "name": "message",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "title",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "description": "The platform on which the post was posted. E.g., Facebook, Instagram, etc. enum (facebook, instagram, reddit)",
            "name": "platform",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "description": "The platform's ID for the post.",
            "name": "platformId",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "description": "The URL to access the post on its platform.",
            "name": "postUrl",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "description": "The number of subscriber the account had when the post was published. This is in contrast to the subscriberCount found on the account, which represents the current number of subscribers an account has.",
            "name": "subscriberCount",
            "type": "INT64",
            "mode": "NULLABLE"
        },
        {
            "description": "The type of the post. enum (album, igtv, link, live_video, live_video_complete, live_video_scheduled, native_video, photo, status, video, vine, youtube)",
            "name": "type",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "description": "The date and time the post was most recently updated in CrowdTangle, which is most often via getting new scores from the platform. Time zone is UTC. \"yyyy-mm-dd hh:mm:ss\")",
            "name": "updated",
            "type": "DATETIME",
            "mode": "REQUIRED"
        },
        {
            "description": "The length of the video in milliseconds.",
            "name": "videoLengthMS",
            "type": "INT64",
            "mode": "NULLABLE"
        },
        {
            "description": "string  The text, if it exists, within an image.",
            "name": "imageText",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "description": "The legacy version of the unique identifier of the post in the CrowdTangle system. This ID is specific to CrowdTangle, not the platform from which the post originated.",
            "name": "legacyId",
            "type": "INT64",
            "mode": "NULLABLE"
        },
        {
            "description": "The caption to a photo, if available.",
            "name": "caption",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "description": "An external URL that the post links to, if available. (Facebook only)",
            "name": "link",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "description": "date (\"yyyy‑mm‑dd hh:mm:ss\")  The date and time the post was published. Time zone is UTC.",
            "name": "date",
            "type": "TIMESTAMP",
            "mode": "NULLABLE"
        },
        {
            "description": "Further details, if available. Associated with links or images across different platforms.",
            "name": "description",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "description": "The score of a post as measured by the request. E.g. it will represent the overperforming score if the request sortBy specifies overperforming, the interaction rate if the request specifies interaction_rate, etc.",
            "name": "score",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },
        {
            "description": "The status of the live video. (\"live\", \"completed\", \"upcoming\")",
            "name": "liveVideoStatus",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "description": "name of file in GCS",
            "name": "file_name",
            "type": "STRING",
            "mode": "NULLABLE"
        }
        ]
        },
    'post_statistics_actual':
            {
                "description": "Actual performance metrics associated with the post.",
                "name": "post_statistics_actual",
                "type": "RECORD",
                "mode": "REQUIRED",
                "fields": [
                    {
                        "description": "format (\"account.id|postExternalId\")   The unique identifier of the post in the CrowdTangle system. This ID is specific to CrowdTangle, not the platform from which the post originated.",
                        "name": "post_id",
                        "type": "STRING",
                        "mode": "REQUIRED"
                    },
                    {
                        "description": "Facebook",
                        "name": "angryCount",
                        "type": "INT64",
                        "mode": "NULLABLE"
                    },
                    {
                        "description": "Facebook, Instagram, Reddit",
                        "name": "commentCount",
                        "type": "INT64",
                        "mode": "NULLABLE"
                    },
                    {
                        "description": "Instagram",
                        "name": "favoriteCount",
                        "type": "INT64",
                        "mode": "NULLABLE"
                    },
                    {
                        "description": "Facebook",
                        "name": "hahaCount",
                        "type": "INT64",
                        "mode": "NULLABLE"
                    },
                    {
                        "description": "Facebook",
                        "name": "likeCount",
                        "type": "INT64",
                        "mode": "NULLABLE"
                    },
                    {
                        "description": "Facebook",
                        "name": "loveCount",
                        "type": "INT64",
                        "mode": "NULLABLE"
                    },
                    {
                        "description": "Facebook",
                        "name": "sadCount",
                        "type": "INT64",
                        "mode": "NULLABLE"
                    },
                    {
                        "description": "Facebook",
                        "name": "shareCount",
                        "type": "INT64",
                        "mode": "NULLABLE"
                    },
                    {
                        "description": "Reddit",
                        "name": "upCount",
                        "type": "INT64",
                        "mode": "NULLABLE"
                    },
                    {
                        "description": "Facebook",
                        "name": "wowCount",
                        "type": "INT64",
                        "mode": "NULLABLE"
                    },
                    {
                        "description": "Facebook",
                        "name": "thankfulCount",
                        "type": "INT64",
                        "mode": "NULLABLE"
                    },
                    {
                        "description": "Facebook",
                        "name": "careCount",
                        "type": "INT64",
                        "mode": "NULLABLE"
                    }
                ]
        },
    'post_statistics_expected': {
                "description": "Expected performance metrics associated with the post.",
                "name": "post_statistics_expected",
                "type": "RECORD",
                "mode": "REQUIRED",
                "fields": [
                {
                    "description": "format (\"account.id|postExternalId\")   The unique identifier of the post in the CrowdTangle system. This ID is specific to CrowdTangle, not the platform from which the post originated.",
                    "name": "post_id",
                    "type": "STRING",
                    "mode": "REQUIRED"
                },
                {
                    "description": "Facebook",
                    "name": "angryCount",
                    "type": "INT64",
                    "mode": "NULLABLE"
                },
                {
                    "description": "Facebook, Instagram, Reddit",
                    "name": "commentCount",
                    "type": "INT64",
                    "mode": "NULLABLE"
                },
                {
                    "description": "Instagram",
                    "name": "favoriteCount",
                    "type": "INT64",
                    "mode": "NULLABLE"
                },
                {
                    "description": "Facebook",
                    "name": "hahaCount",
                    "type": "INT64",
                    "mode": "NULLABLE"
                },
                {
                    "description": "Facebook",
                    "name": "likeCount",
                    "type": "INT64",
                    "mode": "NULLABLE"
                },
                {
                    "description": "Facebook",
                    "name": "loveCount",
                    "type": "INT64",
                    "mode": "NULLABLE"
                },
                {
                    "description": "Facebook",
                    "name": "sadCount",
                    "type": "INT64",
                    "mode": "NULLABLE"
                },
                {
                    "description": "Facebook",
                    "name": "shareCount",
                    "type": "INT64",
                    "mode": "NULLABLE"
                },
                {
                    "description": "Reddit",
                    "name": "upCount",
                    "type": "INT64",
                    "mode": "NULLABLE"
                },
                {
                    "description": "Facebook",
                    "name": "wowCount",
                    "type": "INT64",
                    "mode": "NULLABLE"
                },
                {
                    "description": "Facebook",
                    "name": "thankfulCount",
                    "type": "INT64",
                    "mode": "NULLABLE"
                },
                {
                    "description": "Facebook",
                    "name": "careCount",
                    "type": "INT64",
                    "mode": "NULLABLE"
                }
            ]
        },
    'expanded_links': {
            "description": "List of links as shown to user (original) links that came in the post (which are often shortened), and the expanded links.",
            "name": "expanded_links",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {
                    "description": "format (\"account.id|postExternalId\")   The unique identifier of the post in the CrowdTangle system. This ID is specific to CrowdTangle, not the platform from which the post originated.",
                    "name": "post_id",
                    "type": "STRING",
                    "mode": "REQUIRED"
                },
                {
                    "description": "Expanded version of original link",
                    "name": "expanded",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "description": "original link that came in the post (which are often shortened),",
                    "name": "original",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
            ]
        },
    'media': {
            "description": "Available media for a post.",
            "mode": "REPEATED",
            "name": "media",
            "type": "RECORD",
            "fields": [
                {
                    "description": "format (\"account.id|postExternalId\")   The unique identifier of the post in the CrowdTangle system. This ID is specific to CrowdTangle, not the platform from which the post originated.",
                    "name": "post_id",
                    "type": "STRING",
                    "mode": "REQUIRED"
                },
                {
                    "description": "The source of the full-sized version of the media. API returns this as |full| but that is a reserved word in postgres",
                    "name": "url_full",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "description": "The source of the media.",
                    "name": "url",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "description": "The width of the media.",
                    "name": "width",
                    "type": "INT64",
                    "mode": "NULLABLE"
                },
                {
                    "description": "The height of the media.",
                    "name": "height",
                    "type": "INT64",
                    "mode": "NULLABLE"
                },
                {
                    "description": "The type of the media. enum (photo or video)",
                    "name": "type",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
            ]
        }
}
