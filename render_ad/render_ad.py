from __future__ import annotations

"""Use Selenium to render Facebook ad on the ad library rendering page, take a screenshot, and extract text, links and
images.
"""

from base64 import b64decode
from configparser import ConfigParser
from dataclasses import dataclass
from enum import Enum, unique
from logging import DEBUG, getLogger, INFO, WARNING
from re import compile, Pattern
from selenium.common.exceptions import ElementNotInteractableException, \
    NoSuchElementException, StaleElementReferenceException, TimeoutException, WebDriverException
from selenium.webdriver import Remote
from selenium.webdriver.remote.webelement import WebElement
from tenacity import before_sleep_log, RetryError, Retrying, stop_after_attempt, wait_fixed
from typing import Callable, List, NamedTuple, NewType, Optional, Tuple, Type
from urllib.parse import urlencode

SNAPSHOT_CONTENT_ROOT_XPATH = "//div[@id='content']"

# relative to SNAPSHOT_CONTENT_ROOT_XPATH
CREATIVE_CONTAINER_XPATH = ".//div[contains(concat(' ', @class, ' '), ' _7jyg ') and " \
                           "contains(concat(' ', @class, ' '), ' _7jyi ')]"  # recently _9giw is often (also) present
LOG_IN_MESSAGE_XPATH = ".//div[@class='_cqs']/div/div[text() = 'You must log in to continue.']"
LOG_IN_FORM_XPATH = ".//div[@class='_cqs']/div[@class='_cqq']"
INVALID_API_TOKEN_XPATH = LOG_IN_MESSAGE_XPATH + "|" + LOG_IN_FORM_XPATH

# relative to CREATIVE_CONTAINER_XPATH
MESSAGE_XPATH = "./div[@class='_8mmx _8mmz']/div[@class='_8mlz']/div"
AD_ARCHIVE_ID_XPATH = ".//div[@class='_8nrw']//span[contains(concat(' ', @class, ' '), ' qku1pbnj ')]"
AD_ARCHIVE_ID_PATTERN = compile(r"""ID:\s+(?P<archiveid>\d+)""")
REGULAR_BODY_XPATH = "./div[2]/div[@class='_7jyr']"  # for simple, multi-version, carousel (some video ads may not have it)
RESHARED_OUTER_BODY_XPATH = "./div[2][@class='_7jyl']//div[@class='_7jyr']"
RESHARED_NESTED_CREATIVE_CONTAINER_XPATH = "./div[3][@class='_7jym']"
RESHARED_NESTED_BODY_XPATH = RESHARED_NESTED_CREATIVE_CONTAINER_XPATH + "//div[@class='_7jyr']"
# next two entries relative to CAROUSEL_XPATH or MULTIPLE_VERSION_XPATH, defined out of order for reuse
_CAROUSEL_ENTRY_XPATH_MIDDLE_TEMPLATE_DEFINED_ABOVE_FOR_REUSE = \
    "./div/div/div[@class='_a2e']/div[{carousel_index}][@class='_2zgz']"
_NON_EMPTY_CAROUSEL_XPATH = _CAROUSEL_ENTRY_XPATH_MIDDLE_TEMPLATE_DEFINED_ABOVE_FOR_REUSE.format(carousel_index=1) \
                            + "/../../../.."  # ensure the carousel has an entry (do not detect empty carousels)
# a multiple-version ad shows small images in a carousel for version selection, a click will update main ad area
MULTIPLE_VERSION_XPATH = "./following-sibling::div[@class='_93bv']/div/div[@class='_23n-']" \
                         + "/" + _NON_EMPTY_CAROUSEL_XPATH
# a carousel ad shows large ad creatives (image + link) in a carousel
CAROUSEL_XPATH = "./div[@class='_23n-']" + "/" + _NON_EMPTY_CAROUSEL_XPATH  # usually [3], or [4] if "Ad removed"
RESHARED_NESTED_CAROUSEL_XPATH = RESHARED_NESTED_CREATIVE_CONTAINER_XPATH + "/" + CAROUSEL_XPATH
ADDITIONAL_ASSETS_BUTTON_XPATH = "./following-sibling::div/div/a"
ADDITIONAL_ASSETS_XPATH = "./following-sibling::div/div/div/div/../../following-sibling::div/div"
AD_REMOVED_AREA_XPATH = "./div[2]/div[@class='_7jyo']"

# relative to MULTIPLE_VERSION_XPATH or CAROUSEL_XPATH
CAROUSEL_ENTRY_XPATH_MIDDLE_TEMPLATE = _CAROUSEL_ENTRY_XPATH_MIDDLE_TEMPLATE_DEFINED_ABOVE_FOR_REUSE
# relative to CAROUSEL_XPATH
CAROUSEL_AD_ITEM_XPATH_TEMPLATE = CAROUSEL_ENTRY_XPATH_MIDDLE_TEMPLATE + "/div[@class='_7jy-']"
CAROUSEL_NAVIGATION_ELEMENT_XPATH = ".//a/div[@class='_10sf _5x5_'][@direction='forward']"
# relative to MULTIPLE_VERSION_XPATH
MULTIPLE_VERSION_SELECTOR_ELEMENT_XPATH_TEMPLATE = \
    CAROUSEL_ENTRY_XPATH_MIDDLE_TEMPLATE + "/div[@class='_93bz _93b-' or @class='_93bz']/a/img"  # '_93b-' if selected

# relative to CAROUSEL_AD_ITEM_XPATH_TEMPLATE (carousel) or CREATIVE_CONTAINER_XPATH (simple, multiple)
URL_XPATH = ".//a[contains(concat(' ', @class, ' '), ' d5rc5kzv ')" \
            " and contains(concat(' ', @class, ' '), ' chuaj5k6 ')" \
            " and contains(concat(' ', @class, ' '), ' jrkk970q ')]"
URL_ATTRIBUTE = "href"
# image directly under prefix if no <a>, else child of <a>; first class is for simple/multiple, second for carousel
IMAGE_XPATH = ".//img[@class='_7jys img' or @class='_7jys _7jyt img']"
IMAGE_ATTRIBUTE = "src"
IMAGE_READY_ATTRIBUTE = "complete"
IMAGE_READY_TEST = lambda value: value == "true"
VIDEO_XPATH = ".//div[@class='_8o0a _8o05']/video"  # never wrapped in <a> (link would be next sibling)
VIDEO_IMAGE_ATTRIBUTE = "poster"
VIDEO_ATTRIBUTE = "src"
VIDEO_READY_ATTRIBUTE = "readyState"
VIDEO_READY_TEST = lambda value: int(value) > 0
# for image ads, the <a> is parent of the <img> and the link data <div>
# for video ads, there is a <div> with the <video> before the <a>, which is only a parent of the link data <div>
# for ads without link, the image is a direct child
LINK_TEXT_CONTAINER_XPATH_TEMPLATE = ".//div[@class='{link_area_container_class}']" \
                                     "/div[contains(concat(' ', @class, ' '), ' {link_text_container_class} ')]"
REGULAR_LINK_AREA_CONTAINER_CLASS = " _8jg- _8jg_"  # usually first sibling of image
REGULAR_LINK_TEXT_CONTAINER_CLASS = "b6ewvobd"  # first child of link area container
REGULAR_LINK_TEXT_CONTAINER_XPATH = LINK_TEXT_CONTAINER_XPATH_TEMPLATE.format(
    link_area_container_class=REGULAR_LINK_AREA_CONTAINER_CLASS,
    link_text_container_class=REGULAR_LINK_TEXT_CONTAINER_CLASS,
)
EVENT_LINK_AREA_CONTAINER_CLASS = " _8jtd _8jte"
EVENT_LINK_TEXT_CONTAINER_CLASS = "_8jtf"
EVENT_LINK_TEXT_CONTAINER_XPATH = LINK_TEXT_CONTAINER_XPATH_TEMPLATE.format(
    link_area_container_class=EVENT_LINK_AREA_CONTAINER_CLASS,
    link_text_container_class=EVENT_LINK_TEXT_CONTAINER_CLASS,
)
# relative to CAROUSEL_AD_ITEM_XPATH_TEMPLATE (never observed for simple/multi-version ads, and matches body there)
FOOTER_XPATH = ".//div[@class='_7jyr']/span/div/div[@class='_4ik4 _4ik5']"

# relative to REGULAR_LINK_TEXT_CONTAINER_XPATH or EVENT_LINK_TEXT_CONTAINER_XPATH
LINK_ATTRIBUTE_XPATH_TEMPLATE = "./div[@class='{link_attribute_class}']"
REGULAR_LINK_ATTRIBUTE_XPATH_TEMPLATE = LINK_ATTRIBUTE_XPATH_TEMPLATE + "/div/div[@class='_4ik4 _4ik5']"
REGULAR_LINK_TITLE_CLASS = "_8jh5"
REGULAR_LINK_SECONDARY_TITLE_CLASS = "_94tj"  # rarely present; observed to be "not affiliated with Facebook"
REGULAR_LINK_DESCRIPTION_CLASS = "_8jh2"
REGULAR_LINK_CAPTION_CLASS = "_8jh3"
REGULAR_LINK_SUBCAPTION_CLASS = "_8jh4"  # rarely present; for "Like Page" ads, this has the likes count (smaller font)
# event ads do not have the div[@class='_4ik4 _4ik5']
EVENT_LINK_ATTRIBUTE_XPATH_TEMPLATE = LINK_ATTRIBUTE_XPATH_TEMPLATE
EVENT_LINK_DATE_CLASS = "_8jtg"
EVENT_LINK_DESCRIPTION_CLASS = "_8jm4"
EVENT_LINK_CAPTION_CLASS = "_8jm5"
EVENT_LINK_SECONDARY_CAPTION_CLASS = "_8jm6"  # same font size as caption
# <button>: usually in a <div> sibling to link text container <div>; for event ads direct sibling (not <div>-wrapped)
REGULAR_LINK_BUTTON_CONTAINER_XPATH = "./following-sibling::div[@class='_8jh0']"  # <button> is child of sibling <div>
EVENT_LINK_BUTTON_CONTAINER_XPATH = ".."  # <button> is sibling (not <div>-wrapped)

# relative to REGULAR_LINK_BUTTON_CONTAINER_XPATH or EVENT_LINK_BUTTON_CONTAINER_XPATH
LINK_BUTTON_XPATH = ".//div[@role='button']"

# relative to ADDITIONAL_ASSETS_XPATH
ADDITIONAL_URLS_XPATH = "./div/div/span[text() = 'Links']/../following-sibling::ul/li"
ADDITIONAL_TEXTS_XPATH = "./div/div/span[text() = 'Text']/../following-sibling::ul/li"
ADDITIONAL_IMAGES_XPATH = "./div/div/span[text() = 'Photos' or text() = 'Fotos']/../following-sibling::div//img"
ADDITIONAL_VIDEOS_XPATH = "./div/div/span[text() = 'Videos']/../following-sibling::div//video"

INVALID_ID_ERROR_TEXTS = (
    "Error: Invalid ID\nPlease ensure that the URL is the same as what's in the Graph API response.",
    "Fehler: Ungültige ID\n\"Bitte stelle sicher, dass die URL mit dem Inhalt der Graph API-Antwort übereinstimmt.\"",
)
AGE_RESTRICTION_ERROR_TEXTS = (
    "Because we aren't able to determine your age, we cannot show you this ad.",
    "Because we're unable to determine your age, we cannot show you this ad.",
    "Da wir dein Alter nicht feststellen können, können wir dir diese Werbeanzeige nicht zeigen.",
)
IP_VIOLATION_ERROR_TEXTS = (
    "This ad was taken down because it goes against Facebook's intellectual property policies.",
    "Diese Werbeanzeige wurde entfernt, da sie gegen die Richtlinien für geistiges Eigentum von Facebook verstößt.",
)
TOO_MANY_REQUESTS_ERROR_TEXT = 'You have been temporarily blocked from searching or viewing the Ad Library due to ' \
                               'too many requests. Please try again later.'
AD_REMOVED_TEXTS = (
    'Ad removed',
    'Werbeanzeige entfernt',
)

ArchiveId = NewType('ArchiveId', str)  # Facebook ad archive ID


class AdCreativeLinkAttributes(NamedTuple):
    url: Optional[str]
    title: Optional[str]
    secondary_title: Optional[str]
    description: Optional[str]
    caption: Optional[str]
    secondary_caption: Optional[str]
    button: Optional[str]


class AdImage(NamedTuple):
    url: Optional[str]
    binary_data: Optional[bytes]


class AdVideo(NamedTuple):
    url: Optional[str]
    poster_image: Optional[AdImage]


class FetchedAdCreativeData(NamedTuple):
    body: Optional[str]
    link_attributes: Optional[AdCreativeLinkAttributes]
    image: Optional[AdImage]
    video_url: Optional[str]
    footer: Optional[str]
    reshared_outer_body: Optional[str]


@unique
class AdType(Enum):
    SIMPLE: str = 'simple'
    MULTIPLE_VERSIONS: str = 'multiple'
    CAROUSEL: str = 'carousel'
    RESHARED: str = 'reshared'


class AdScreenshotAndCreatives(NamedTuple):
    screenshot_binary_data: bytes
    creatives: List[FetchedAdCreativeData]
    ad_type: AdType
    extra_urls: List[str]
    extra_texts: List[str]
    extra_images: List[AdImage]
    extra_videos: List[AdVideo]
    has_ad_removed_label: bool


class Error(Exception):
    """Generic error type for this module."""

    def __init__(self, *args):
        super().__init__(*args)
        self.partial_result: Optional[AdScreenshotAndCreatives] = None


class BrowserTimeoutError(Error):
    """Raised for browser/Selenium timeouts."""


class TooManyRequestsError(Error):
    """raised when the retriever is told it has made too many requests too quickly."""


class SnapshotNoContentFoundError(Error):
    """Raised if unable to find content in fetched snapshot."""


class SnapshotPermanentlyUnavailableError(Error):
    """Superclass for exceptions in cases where ad is likely permanently unavailable."""

    class Reason(Enum):
        INVALID_ID: str = 'invalid_id'
        AGE_RESTRICTION: str = 'restricted_age'
        INTELLECTUAL_PROPERTY_VIOLATION: str = 'restricted_ip_violation'
        OTHER_RESTRICTION: str = 'restricted_other'

    def __init__(self, reason: Reason, *args):
        super().__init__(*args)
        self.reason = reason


class SnapshotInvalidIdError(SnapshotPermanentlyUnavailableError):
    """Raised if fetched snapshot has Invalid ID error message."""

    def __init__(self, *args):
        super().__init__(SnapshotPermanentlyUnavailableError.Reason.INVALID_ID, *args)


class SnapshotInvalidApiTokenError(Error):
    """Raised if the provided API token is invalid/expired."""


class SnapshotAgeRestrictionError(SnapshotPermanentlyUnavailableError):
    """Raised if fetched Snapshot has age restriction error message."""

    def __init__(self, *args):
        super().__init__(SnapshotPermanentlyUnavailableError.Reason.AGE_RESTRICTION, *args)


class SnapshotIntellectualPropertyViolationError(SnapshotPermanentlyUnavailableError):
    """Raised if the fetched snapshot has an intellectual property violation error message."""

    def __init__(self, *args):
        super().__init__(SnapshotPermanentlyUnavailableError.Reason.INTELLECTUAL_PROPERTY_VIOLATION, *args)


class SnapshotOtherRestrictionError(SnapshotPermanentlyUnavailableError):
    """Raised if the fetched snapshot has an unrecognised restriction error message."""

    def __init__(self, *args):
        super().__init__(SnapshotPermanentlyUnavailableError.Reason.OTHER_RESTRICTION, *args)


class SnapshotWrongAdArchiveIdError(Error):
    """Raised if fetched snapshot has unexpected ad archive ID (or it cannot be extracted)."""


class SnapshotUnknownAdTypeError(Error):
    """Raised if the type of the ad could not be determined."""


class SnapshotMissingMediaError(Error):
    """Raised if an image exists but could not be downloaded/extracted"""


class SnapshotNextVersionSelectionError(Error):
    """Raised when unable to select the next version of a carousel or multiple-version ad."""


class SnapshotRenderingTemplateMissingModuleError(Error):
    """Raised if the local rendering template is missing a JavaScript module required to correctly display the ad."""


@dataclass
class BrowserConsoleLogTest:
    pattern: Pattern
    log_message: str
    log_level: int
    exception: Optional[Type[Error]]


VIDEO_LOAD_FAILED_TEST_LOG_ONLY = BrowserConsoleLogTest(
    pattern=compile(
        r"""https?://video[^ ]+\s+-\s+Failed to load resource: the server responded with a status of (403|404|410)"""),
    log_message='A video failed to load (403/404/410), its poster image might still be available',
    log_level=INFO,
    exception=None,  # allow video load failures - only extracting poster image anyway
)
IMAGE_LOAD_FAILED_TEST_LOG_ONLY = BrowserConsoleLogTest(
    pattern=compile(r"""https?://scontent[^ ]+_n\.jpg\?[^ ]+\s+-\s+Failed to load resource: the server responded with"""
                    """ a status of 410"""),
    log_message='An image failed to load - do not treat as error as per configuration (could indicate that CDN links '
                'in ad data used for local rendering have expired)',
    log_level=INFO,
    exception=None,  # allow image load failures
)
RESOURCE_LOAD_FAILED_TEST = BrowserConsoleLogTest(
    # this test appears more reliable than the prior test of attempting to extract the profile picture - sometimes the
    # profile picture might still work, whereas other resources cannot be loaded
    pattern=compile(r"""https?://[^ ]+\s+-\s+Failed to load resource: the server responded with a status of \d+"""),
    log_message='A resource failed to load (could indicate that CDN links in ad data used for local rendering have '
                'expired)',
    log_level=DEBUG,
    exception=SnapshotMissingMediaError,
)
CUSTOM_INJECTED_STYLES = """
    /* hide cookie dialogue */
    body div[data-testid='cookie-policy-dialog'] {
        display: none;
    }

    /* hide tooltips (can obstruct/intercept buttons or selectors) */
    body div[data-testid='ContextualLayerRoot'] {
        display: none;
    }

    /* ensure ad version selectors are "visible" when corresponding images fail to load */
    div._2zgz img {
        min-height: 1px;
        min-width: 1px;
    }
"""

log = getLogger(__name__)

FB_AD_SNAPSHOT_BASE_URL_TEMPLATE = 'https://www.facebook.com/ads/archive/render_ad/?{}'


def get_api_url(archive_id: ArchiveId, access_token: str) -> str:
    return FB_AD_SNAPSHOT_BASE_URL_TEMPLATE.format(urlencode({
        'id': archive_id,
        'access_token': access_token,
    }))


def is_empty(string: Optional[str]) -> bool:
    return string is None or len(string.strip()) == 0


def unify_string(string: Optional[str]) -> Optional[str]:
    if is_empty(string):
        return None
    return string.strip()


class FacebookAdCreativeRetrieverFactory:
    """Singleton"""

    def __init__(self, config: ConfigParser):
        self._config = config

    def build(self, chrome_driver: Remote) -> 'FacebookAdCreativeRetriever':
        return FacebookAdCreativeRetriever(
            self._config,
            chrome_driver,
        )


class FacebookAdCreativeRetriever:
    """A new instance built for each new Selenium web driver connection/instance, sharing all metrics via factory"""

    def __init__(
            self,
            config: ConfigParser,
            chrome_driver: Remote,
    ):
        config_section = config['adsnapshots']
        self._chrome_driver = chrome_driver
        self._access_token = config_section['fb_access_token']
        self._element_poll_interval_seconds = config_section.getfloat('element_poll_interval_seconds')
        self._next_element_selection_max_arrow_clicks = config_section.getint('next_element_selection_max_arrow_clicks')
        self._next_element_selection_max_element_visibility_polling_attempts = config_section.getint(
            'next_element_selection_max_element_visibility_polling_attempts')
        self._next_element_selection_max_selector_click_attempts = config_section.getint(
            'next_element_selection_max_selector_click_attempts')
        self._image_extraction_max_readiness_polling_attempts = config_section.getint(
            'image_extraction_max_readiness_polling_attempts')
        self._video_preview_extraction_max_readiness_polling_attempts = config_section.getint(
            'video_preview_extraction_max_readiness_polling_attempts')
        self._button_max_availability_polling_attempts = config_section.getint(
            'button_max_availability_polling_attempts')
        self._additional_assets_drawer_max_availability_polling_attempts = config_section.getint(
            'additional_assets_drawer_max_availability_polling_attempts')
        self._max_carousel_items = config_section.getint('max_carousel_items')
        raise_if_network_error_logged_to_console = config_section.getboolean(
            'raise_if_network_error_logged_to_console')
        self._raise_if_missing_image = config_section.getboolean('raise_if_missing_image')

        self._main_frame_id = None
        self._browser_console_log_tests = []
        self._browser_console_log_tests.append(VIDEO_LOAD_FAILED_TEST_LOG_ONLY)
        if raise_if_network_error_logged_to_console:
            if not self._raise_if_missing_image:
                self._browser_console_log_tests.append(IMAGE_LOAD_FAILED_TEST_LOG_ONLY)
            self._browser_console_log_tests.append(RESOURCE_LOAD_FAILED_TEST)

    def retrieve_ad(self, archive_id: ArchiveId) -> AdScreenshotAndCreatives:
        try:
            result = self._do_retrieve_ad(archive_id)
            self._compute_creative_metrics(archive_id, result)
            return result
        except TooManyRequestsError:
            log.info("Too many requests error when retrieving ad archive ID '%s'", archive_id)
            raise
        except SnapshotNoContentFoundError:
            log.info("No content found in page when retrieving ad archive ID '%s'", archive_id, exc_info=True)
            raise
        except SnapshotPermanentlyUnavailableError as e:
            log.info("Ad archive ID '%s' is permanently unavailable: %s", archive_id, e.reason.value)
            raise
        except SnapshotInvalidApiTokenError:
            log.error("Cannot retrieve ad archive ID '%s' (API token invalid)", archive_id)
            raise
        except SnapshotWrongAdArchiveIdError:
            log.info("Rendering page did not display expected ad archive ID '%s'", archive_id)
            raise
        except SnapshotUnknownAdTypeError:
            log.warning("Could not determine ad type for ad archive ID '%s'", archive_id)
            raise
        except SnapshotMissingMediaError:
            log.info("Could not extract image from browser for ad archive ID '%s'", archive_id)
            raise
        except SnapshotNextVersionSelectionError:
            log.info("Could not select next version for ad archive ID '%s'", archive_id, exc_info=True)
            raise
        except Error:
            log.warning("Unspecified data extraction issue for ad archive ID '%s'", archive_id,
                        exc_info=True)
            raise
        except TimeoutException as e:
            log.info("Browser timeout for ad archive ID '%s'", archive_id, exc_info=True)
            raise BrowserTimeoutError() from e
        except WebDriverException:
            log.warning("Selenium error when retrieving ad archive ID '%s'", archive_id, exc_info=True)
            raise

    def _compute_creative_metrics(self, archive_id: ArchiveId, result: AdScreenshotAndCreatives) -> None:
        # assuming exceptions are raised for ads with missing screenshots or no creatives (not counted here)
        if len(result.creatives) == 0:
            log.info("Found no ad creatives for ad archive ID '%s'", archive_id)

        no_body = {c for c in result.creatives if is_empty(c.body)}
        no_image_url = {c for c in result.creatives if c.image is None or is_empty(c.image.url)}
        image_url_no_data = {c for c in result.creatives if c not in no_image_url and (
                c.image.binary_data is None or len(c.image.binary_data) == 0)}
        missing_link_attributes = {c for c in result.creatives if c.link_attributes is None}
        no_data_at_all = no_body & no_image_url & missing_link_attributes

        if len(no_data_at_all) > 0:
            if any([creative.video_url is not None for creative in result.creatives]):
                # some video ads have no text/link (do not warn)
                log.info("Video ad with archive ID '%s' has creative(s) without any extracted metadata", archive_id)
            else:
                log.warning("Non-video ad with archive ID '%s' has creative(s) without any extracted metadata",
                            archive_id)
        elif len(missing_link_attributes) > 0:
            log.debug("Ad with archive ID '%s' has creative(s) without detected link attributes", archive_id)
        if len(no_body) > 0:
            log.debug("Ad with archive ID '%s' has creative(s) without text/body", archive_id)
        if len(no_image_url) > 0 or len(image_url_no_data) > 0:
            log.info("Ad with archive ID '%s' has creative(s) without image", archive_id)

    def _do_retrieve_ad(self, archive_id: ArchiveId) -> AdScreenshotAndCreatives:
        log.debug("Fetching ad archive ID '%s'", archive_id)
        self._chrome_driver.get(self._get_url(archive_id))
        self._inject_custom_styles()
        content_root_element = self._find_content_root()
        self._raise_if_invalid_api_token(content_root_element)
        creative_container_element = self._get_creative_container_element(content_root_element)

        try:
            return self._extract_ad_from_page(archive_id, creative_container_element)
        except Error as e:
            e.partial_result = AdScreenshotAndCreatives(
                screenshot_binary_data=None,
                creatives=None,
                ad_type=None,
                extra_urls=None,
                extra_texts=None,
                extra_images=None,
                extra_videos=None,
                has_ad_removed_label=None,
            )
            raise e

    def _extract_ad_from_page(self, archive_id: ArchiveId, creative_container_element: WebElement) \
            -> AdScreenshotAndCreatives:
        self._raise_if_wrong_id(archive_id, creative_container_element)
        self._raise_if_ad_restricted(creative_container_element)
        self._raise_if_error_logged_to_console(archive_id)

        ad_type, body_element, carousel_element, reshared_outer_body_element = self._find_ad_type_elements(
            creative_container_element)
        log.debug("Ad is of type: %s", ad_type.value)

        ad_removed_label = self._extract_ad_removed_label(creative_container_element)
        screenshot = creative_container_element.screenshot_as_png
        if ad_type is AdType.CAROUSEL:
            creatives = self._extract_creatives(creative_container_element, body_element, carousel_element, True, None)
        elif ad_type in (AdType.SIMPLE, AdType.MULTIPLE_VERSIONS):
            creatives = self._extract_creatives(creative_container_element, body_element, carousel_element, False, None)
        elif ad_type is AdType.RESHARED:
            creatives = self._extract_creatives(creative_container_element, body_element, carousel_element,
                                                carousel_element is not None, reshared_outer_body_element)
        else:
            raise SnapshotUnknownAdTypeError()
        extra_urls, extra_texts, extra_images, extra_videos = self._extract_additional_assets(
            creative_container_element)
        log.debug("Done fetching ad archive ID '%s' (%s), extracted %d creative(s)", archive_id, ad_type.value,
                  len(creatives))
        return AdScreenshotAndCreatives(
            screenshot_binary_data=screenshot,
            creatives=creatives,
            ad_type=ad_type,
            extra_urls=extra_urls,
            extra_texts=extra_texts,
            extra_images=extra_images,
            extra_videos=extra_videos,
            has_ad_removed_label=ad_removed_label,
        )

    def _get_url(self, archive_id: ArchiveId) -> str:
        return get_api_url(archive_id, self._access_token)

    def _inject_custom_styles(self) -> None:
        self._chrome_driver.execute_script("""
            const element = document.createElement('style');
            element.textContent = arguments[0];
            document.head.append(element);
        """, CUSTOM_INJECTED_STYLES)

    def _find_content_root(self) -> WebElement:
        try:
            return self._chrome_driver.find_element_by_xpath(SNAPSHOT_CONTENT_ROOT_XPATH)
        except NoSuchElementException:
            page_text = self._chrome_driver.find_element_by_tag_name('html').text.lower()
            if TOO_MANY_REQUESTS_ERROR_TEXT.lower() in page_text:
                raise TooManyRequestsError()
            log.debug('Could not find content in page:\n%s', page_text)
            raise SnapshotNoContentFoundError()

    def _raise_if_invalid_api_token(self, content_root_element: WebElement) -> None:
        try:
            content_root_element.find_element_by_xpath(INVALID_API_TOKEN_XPATH)
            raise SnapshotInvalidApiTokenError()
        except NoSuchElementException:
            pass

    def _get_creative_container_element(self, content_root_element: WebElement) -> WebElement:
        try:
            return content_root_element.find_element_by_xpath(CREATIVE_CONTAINER_XPATH)
        except NoSuchElementException as e:
            # maybe the creative container is missing because the ad archive ID is invalid
            error_text = content_root_element.text.lower()
            for invalid_id_error_text in INVALID_ID_ERROR_TEXTS:
                if invalid_id_error_text.lower() in error_text:
                    raise SnapshotInvalidIdError()
            raise SnapshotNoContentFoundError() from e

    def _raise_if_wrong_id(self, archive_id: ArchiveId, creative_container_element: WebElement) -> None:
        # consistency check to ensure the ad has the expected archive ID
        try:
            id_text = self._get_node_text(AD_ARCHIVE_ID_XPATH, creative_container_element)
            match = AD_ARCHIVE_ID_PATTERN.search(id_text)
            assert match.group('archiveid') == archive_id
        except (AssertionError, IndexError, NoSuchElementException, TypeError):
            raise SnapshotWrongAdArchiveIdError()

    def _raise_if_ad_restricted(self, creative_container_element: WebElement) -> None:
        message_text = self._get_node_text(MESSAGE_XPATH, creative_container_element)
        if message_text is None:
            return
        for error_texts, exception_type in (
                (AGE_RESTRICTION_ERROR_TEXTS, SnapshotAgeRestrictionError),
                (IP_VIOLATION_ERROR_TEXTS, SnapshotIntellectualPropertyViolationError),
        ):
            for error_text in error_texts:
                if error_text.lower() in message_text.lower():
                    raise exception_type()
        log.warning("Page contains an unrecognised message instead of the ad: '%s'", message_text)
        raise SnapshotOtherRestrictionError()

    def _raise_if_error_logged_to_console(self, archive_id: ArchiveId) -> None:
        browser_log = self._chrome_driver.get_log('browser')
        for log_entry in browser_log:
            for test in self._browser_console_log_tests:
                match = test.pattern.search(log_entry['message'])
                if not match:
                    continue
                log.log(test.log_level, "Browser logged error during rendering of ad archive ID '%s': %s. Original "
                                        "console entry: '%s'", archive_id, test.log_message, log_entry['message'])
                if test.exception is not None:
                    raise test.exception()
                break  # if no exception, do not apply other tests and check next log entry

    def _find_ad_type_elements(self, creative_container_element: WebElement) \
            -> Tuple[AdType, Optional[WebElement], Optional[WebElement], Optional[WebElement]]:
        try:
            return self._find_reshared_elements(creative_container_element)
        except NoSuchElementException:
            pass
        body_element = self._find_regular_body_element(creative_container_element)
        try:
            multiple_version_element = creative_container_element.find_element_by_xpath(MULTIPLE_VERSION_XPATH)
            return AdType.MULTIPLE_VERSIONS, body_element, multiple_version_element, None
        except NoSuchElementException:
            pass
        try:
            carousel_element = creative_container_element.find_element_by_xpath(CAROUSEL_XPATH)
            return AdType.CAROUSEL, body_element, carousel_element, None
        except NoSuchElementException:
            pass
        if body_element is None:
            self._raise_if_no_simple_ad_element_detected(creative_container_element)
        return AdType.SIMPLE, body_element, None, None

    def _find_reshared_elements(self, creative_container_element: WebElement) \
            -> Tuple[AdType, WebElement, Optional[WebElement], Optional[WebElement]]:
        outer_body_element = creative_container_element.find_element_by_xpath(RESHARED_OUTER_BODY_XPATH)
        nested_body_element = creative_container_element.find_element_by_xpath(RESHARED_NESTED_BODY_XPATH)
        try:
            nested_carousel_element = creative_container_element.find_element_by_xpath(RESHARED_NESTED_CAROUSEL_XPATH)
        except NoSuchElementException:
            nested_carousel_element = None
        return AdType.RESHARED, nested_body_element, nested_carousel_element, outer_body_element

    def _find_regular_body_element(self, creative_container_element: WebElement) -> Optional[WebElement]:
        try:
            return creative_container_element.find_element_by_xpath(REGULAR_BODY_XPATH)
        except NoSuchElementException:
            return None  # some ads have no body at all (not even empty)

    def _raise_if_no_simple_ad_element_detected(self, creative_container_element: WebElement) -> None:
        for xpath in (VIDEO_XPATH, IMAGE_XPATH):
            try:
                creative_container_element.find_element_by_xpath(xpath)
                return
            except NoSuchElementException:
                pass
        raise SnapshotUnknownAdTypeError()  # ad has neither body, nor video or image

    def _extract_additional_assets(self, creative_container_element: WebElement) \
            -> Tuple[List[str], List[str], List[AdImage], List[AdVideo]]:
        try:
            assets_button = self._get_node(ADDITIONAL_ASSETS_BUTTON_XPATH, creative_container_element)
        except NoSuchElementException:
            return [], [], [], []
        self._do_click(assets_button, "Additional assets button")
        try:
            additional_assets_element = self._get_node(ADDITIONAL_ASSETS_XPATH, creative_container_element,
                                                       self._additional_assets_drawer_max_availability_polling_attempts)
        except (NoSuchElementException, RetryError):
            log.warning("Could not expand drop-down for additional assets, not extracting additional assets")
            return [], [], [], []
        extra_urls = self._get_node_texts(ADDITIONAL_URLS_XPATH, additional_assets_element)
        extra_texts = self._get_node_texts(ADDITIONAL_TEXTS_XPATH, additional_assets_element)
        extra_images = [self._extract_image_data(image_element, IMAGE_ATTRIBUTE, IMAGE_READY_ATTRIBUTE,
                                                 IMAGE_READY_TEST,
                                                 self._image_extraction_max_readiness_polling_attempts,
                                                 self._raise_if_missing_image)
                        for image_element in additional_assets_element.find_elements_by_xpath(ADDITIONAL_IMAGES_XPATH)]
        extra_videos = [self._extract_video_data(video_element)
                        for video_element in additional_assets_element.find_elements_by_xpath(ADDITIONAL_VIDEOS_XPATH)]
        return extra_urls, extra_texts, extra_images, extra_videos

    def _extract_ad_removed_label(self, creative_container_element: WebElement) -> bool:
        ad_removed_text = self._get_node_text(AD_REMOVED_AREA_XPATH, creative_container_element)
        if ad_removed_text is None:
            return False
        elif ad_removed_text in AD_REMOVED_TEXTS:
            return True
        log.warning("Ad has area for 'Ad removed' label, but the label itself had an unexpected text: '%s'",
                    ad_removed_text)
        return False

    def _extract_creatives(self, creative_container_element: WebElement, body_element: WebElement,
                           carousel_element: Optional[WebElement], is_carousel_ad: bool,
                           reshared_outer_body_element: Optional[WebElement]) -> List[FetchedAdCreativeData]:
        # carousel ads have a shared ad body, and a carousel of ad images/videos/links (for display); some dynamic
        #  carousel ads have a shared link area instead of one per carousel item
        # reshared ads have an outer body with only text, and an inner ad body, an optional inner image-only carousel,
        #  and a shared inner link area
        # multiple-version ads have a single ad content area, and a small carousel to select a version to show.
        #  Some versions may not have a body element while others do; this requires re-extracting the element each time
        # simple ads are similar to multiple-version ads, but do not have the version selector carousel
        # to select next creative version:
        # - carousel ad: click arrow until next carousel item visible, then extract creative from item
        # - for reshared ads, extract as carousel/simple ad depending on whether they have the multiple-image carousel;
        #   also extract additional outer body
        # - multiple-version ad: click arrow until next version carousel item visible, then click on item to display
        #   this version, and extract creative from (shared) creative container
        # - for simple (single-version) ad, carousel_element is None
        creatives = []
        reshared_outer_body = self._extract_body(reshared_outer_body_element)
        if is_carousel_ad:
            creative_body = self._extract_body(body_element)
            creative_parent_element = None
            fallback_parent_element = creative_container_element
            assert carousel_element is not None
        else:
            creative_body = None
            creative_parent_element = creative_container_element
            fallback_parent_element = None

        for carousel_index in range(1, self._max_carousel_items + 1):
            if is_carousel_ad:
                carousel_item_xpath_template = CAROUSEL_AD_ITEM_XPATH_TEMPLATE
            else:
                carousel_item_xpath_template = MULTIPLE_VERSION_SELECTOR_ELEMENT_XPATH_TEMPLATE
            try:
                carousel_item_element = carousel_element.find_element_by_xpath(
                    carousel_item_xpath_template.format(carousel_index=carousel_index))
            except AttributeError:
                carousel_item_element = None  # when carousel_element is None (single-version ad)
            except NoSuchElementException:
                break  # end when no more carousel items exist

            if carousel_index > 1 and carousel_item_element is not None:  # bring next carousel item into view
                self._click_carousel_arrow_until_item_visible(carousel_element, carousel_item_element, carousel_index)
                if not is_carousel_ad:  # for multiple-version ad, also click to select ad version for display
                    self._click_carousel_item(carousel_item_element)

            if is_carousel_ad:
                creative_parent_element = carousel_item_element
            else:
                if carousel_index > 1:  # re-extract for multi-version ads (sometimes element removed/re-added)
                    body_element = self._find_regular_body_element(creative_container_element)
                creative_body = self._extract_body(body_element)
            creative = self._extract_creative_data(creative_body, reshared_outer_body, creative_parent_element,
                                                   fallback_parent_element, is_carousel_ad)
            if creative is not None:
                creatives.append(creative)
            if carousel_element is None or creative is None:
                break  # single-version ad, or all-empty ad
        return creatives

    def _extract_body(self, body_element: Optional[WebElement]) -> Optional[str]:
        if body_element is None:
            return None
        return unify_string(body_element.text)

    def _click_carousel_arrow_until_item_visible(self, carousel_element: WebElement, carousel_item_element: WebElement,
                                                 carousel_index: int) -> None:
        if carousel_item_element.is_displayed():
            return
        try:
            navigation_element = carousel_element.find_element_by_xpath(CAROUSEL_NAVIGATION_ELEMENT_XPATH)
        except NoSuchElementException:
            log.info("Cannot click on overflow navigation arrow to show item #%d: arrow not found", carousel_index)
            raise SnapshotNextVersionSelectionError()
        try:
            for click_attempt in Retrying(before_sleep=before_sleep_log(log, DEBUG),
                                          stop=stop_after_attempt(self._next_element_selection_max_arrow_clicks)):
                with click_attempt:
                    try:
                        self._do_click(navigation_element, "Carousel navigation arrow")
                    except StaleElementReferenceException:
                        pass  # arrow not available any more (removed when end of carousel reached)
                    for status_poll_attempt in self._retry_polling(
                            self._next_element_selection_max_element_visibility_polling_attempts):
                        with status_poll_attempt:
                            assert carousel_item_element.is_displayed(), \
                                "The expected carousel item #{} is not displayed yet".format(carousel_index)
                            return
                    # RetryError from unsuccessful polling will force next click attempt
        except RetryError:
            pass
        log.info("Maximum attempts exceeded when clicking overflow arrow to make carousel item #%d visible",
                 carousel_index)
        raise SnapshotNextVersionSelectionError()

    def _click_carousel_item(self, carousel_item_element: WebElement) -> None:
        # use JavaScript to click on ad version selector (generally not advisable, but more reliable when missing img)
        self._chrome_driver.execute_script('arguments[0].click();', carousel_item_element)

    def _do_click(self, element: WebElement, log_element_description: str) -> None:
        try:
            element.click()
            return
        except ElementNotInteractableException:
            log.debug("%s is not interactable (likely zero size) - try scrolling it into view", log_element_description)
            # this can happen for ads larger than the browser window
            self._chrome_driver.execute_script('arguments[0].scrollIntoView(false);', element)
        try:
            element.click()
            return
        except ElementNotInteractableException:
            log.warning("%s is still not interactable - try clicking it with JavaScript (may not work as expected)",
                        log_element_description, exc_info=True)
            self._chrome_driver.execute_script('arguments[0].click();', element)

    def _extract_creative_data(self, creative_body: Optional[str], reshared_outer_body: Optional[str],
                               parent_element: WebElement, fallback_parent_element: Optional[WebElement],
                               extract_footer: bool) \
            -> Optional[FetchedAdCreativeData]:
        # some ads have only links, only images, or images with links
        link_attributes = self._extract_link_attributes(parent_element)
        if link_attributes is None and fallback_parent_element is not None:
            # some dynamic ads have an image carousel, but the link attribute area is shared
            link_attributes = self._extract_link_attributes(fallback_parent_element)
        image, video_url = self._extract_image_or_video(parent_element)
        if extract_footer:
            footer_text = self._get_node_text(FOOTER_XPATH, parent_element)
        else:
            footer_text = None
        if is_empty(creative_body) and link_attributes is None and image is None and video_url is None \
                and is_empty(footer_text) and is_empty(reshared_outer_body):
            return None
        return FetchedAdCreativeData(
            body=creative_body,
            link_attributes=link_attributes,
            image=image,
            video_url=video_url,
            footer=footer_text,
            reshared_outer_body=reshared_outer_body,
        )

    def _extract_link_attributes(self, parent_element: WebElement) -> Optional[AdCreativeLinkAttributes]:
        creative_link_url = self._get_node_attribute(URL_XPATH, URL_ATTRIBUTE, parent_element)
        # some dynamic ads have "link" attributes/a dynamic form without URL
        for link_text_container_xpath, link_attribute_xpath_template, title_class, secondary_title_class, \
            description_class, caption_class, secondary_caption_class, link_button_container_xpath in (
                (REGULAR_LINK_TEXT_CONTAINER_XPATH, REGULAR_LINK_ATTRIBUTE_XPATH_TEMPLATE, REGULAR_LINK_TITLE_CLASS,
                 REGULAR_LINK_SECONDARY_TITLE_CLASS,
                 REGULAR_LINK_DESCRIPTION_CLASS, REGULAR_LINK_CAPTION_CLASS, REGULAR_LINK_SUBCAPTION_CLASS,
                 REGULAR_LINK_BUTTON_CONTAINER_XPATH),
                (EVENT_LINK_TEXT_CONTAINER_XPATH, EVENT_LINK_ATTRIBUTE_XPATH_TEMPLATE, EVENT_LINK_DATE_CLASS, None,
                 EVENT_LINK_DESCRIPTION_CLASS,
                 EVENT_LINK_CAPTION_CLASS, EVENT_LINK_SECONDARY_CAPTION_CLASS, EVENT_LINK_BUTTON_CONTAINER_XPATH),
        ):
            link_attributes = self._get_link_attributes_for_xpath_template(parent_element, link_text_container_xpath,
                                                                           link_attribute_xpath_template,
                                                                           title_class, secondary_title_class,
                                                                           description_class, caption_class,
                                                                           secondary_caption_class,
                                                                           link_button_container_xpath,
                                                                           creative_link_url)
            if link_attributes is not None:
                return link_attributes
        if creative_link_url is not None:
            return AdCreativeLinkAttributes(url=creative_link_url, title=None, secondary_title=None, description=None,
                                            caption=None, secondary_caption=None, button=None)
        return None

    def _get_link_attributes_for_xpath_template(self, parent_element: WebElement,
                                                link_text_container_xpath: str,
                                                link_attribute_xpath_template: str,
                                                title_class: str, secondary_title_class: Optional[str],
                                                description_class: str, caption_class: str,
                                                secondary_caption_class: str, link_button_container_xpath: str,
                                                url: str) -> Optional[AdCreativeLinkAttributes]:
        try:
            link_text_container_element = parent_element.find_element_by_xpath(link_text_container_xpath)
        except NoSuchElementException:
            return None
        title = self._get_node_text(link_attribute_xpath_template.format(link_attribute_class=title_class),
                                    link_text_container_element)
        if secondary_title_class is not None:
            secondary_title = self._get_node_text(
                link_attribute_xpath_template.format(link_attribute_class=secondary_title_class),
                link_text_container_element)
        else:
            secondary_title = None
        description = self._get_node_text(
            link_attribute_xpath_template.format(link_attribute_class=description_class), link_text_container_element)
        caption = self._get_node_text(
            link_attribute_xpath_template.format(link_attribute_class=caption_class), link_text_container_element)
        secondary_caption = self._get_node_text(
            link_attribute_xpath_template.format(link_attribute_class=secondary_caption_class),
            link_text_container_element)
        try:
            # wait for button to appear (not there immediately), but only if its container exists
            button_container_element = link_text_container_element.find_element_by_xpath(link_button_container_xpath)
            button = self._get_node_text(LINK_BUTTON_XPATH, button_container_element,
                                         self._button_max_availability_polling_attempts)
        except NoSuchElementException:
            button = None
        if title is None and secondary_title is None and description is None \
                and caption is None and secondary_caption is None and button is None:
            return None
        return AdCreativeLinkAttributes(
            url=url,
            title=title,
            secondary_title=secondary_title,
            description=description,
            caption=caption,
            secondary_caption=secondary_caption,
            button=button,
        )

    def _extract_image_or_video(self, parent_element: WebElement) -> Tuple[Optional[AdImage], Optional[str]]:
        try:
            image_element = self._get_node(IMAGE_XPATH, parent_element)
            image = self._extract_image_data(image_element, IMAGE_ATTRIBUTE, IMAGE_READY_ATTRIBUTE, IMAGE_READY_TEST,
                                             self._image_extraction_max_readiness_polling_attempts,
                                             self._raise_if_missing_image)
            return image, None
        except NoSuchElementException:
            pass
        try:
            video_element = self._get_node(VIDEO_XPATH, parent_element)
        except NoSuchElementException:
            return None, None
        video = self._extract_video_data(video_element)
        return (video.poster_image, video.url) if video is not None else (None, None)

    def _extract_image_data(self, image_element: WebElement, url_attribute_name: str, image_ready_attribute_name: str,
                            image_ready_test: Callable[[str], bool], max_attempts: int, raise_if_not_ready: bool) \
            -> Optional[AdImage]:
        image_url = self._get_element_attribute_url(image_element, url_attribute_name)
        if image_url is None:
            return None
        is_ready = self._wait_for_image_ready(image_element, image_url, image_ready_attribute_name, image_ready_test,
                                              max_attempts, raise_if_not_ready)
        image_data = self._extract_media_data_from_browser(image_url)
        if not is_ready:
            log.info("Extracted image from browser even though it might not have finished downloading (detection is "
                     "unreliable for poster images of video ads). URL: '%s'", image_url)
        return AdImage(
            url=image_url,
            binary_data=image_data,
        )

    def _extract_video_data(self, video_element: WebElement) -> Optional[AdVideo]:
        poster_image = self._extract_image_data(video_element, VIDEO_IMAGE_ATTRIBUTE, VIDEO_READY_ATTRIBUTE,
                                                VIDEO_READY_TEST,
                                                self._video_preview_extraction_max_readiness_polling_attempts, False)
        video_url = self._get_element_attribute_url(video_element, VIDEO_ATTRIBUTE)
        if poster_image is None and video_url is None:
            return None

        return AdVideo(
            url=video_url,
            poster_image=poster_image,
        )

    def _get_element_attribute_url(self, element: WebElement, url_attribute_name: str) -> Optional[str]:
        url = unify_string(element.get_attribute(url_attribute_name))
        if url is None or (url.startswith(FB_AD_SNAPSHOT_BASE_URL_TEMPLATE.format(""))
                           and url == self._chrome_driver.current_url):
            # possible browser bug returning page URL instead of empty URL for non-existing video poster attribute
            return None
        return url

    def _wait_for_image_ready(self, image_element: WebElement, image_url: str, image_ready_attribute_name: str,
                              image_ready_test: Callable[[str], bool], max_attempts: int, raise_if_not_ready: bool) \
            -> bool:
        try:
            for attempt in self._retry_polling(max_attempts):
                with attempt:
                    assert image_ready_test(image_element.get_attribute(image_ready_attribute_name)), \
                        "The image element has not completed downloading yet. URL: '{}'".format(image_url)
            return True
        except RetryError:
            if raise_if_not_ready:
                raise SnapshotMissingMediaError()
            return False
            # for video ads, detection might not be reliable since it is based on the video, not the poster (CDN links
            # for video seem to expire within a few days, will cause readyState to remain at 0 even if poster available)

    def _get_node_text(self, xpath: str, parent_element: Optional[WebElement] = None,
                       max_attempts: int = 1) -> Optional[str]:
        try:
            return unify_string(self._get_node(xpath, parent_element, max_attempts).text)
        except (NoSuchElementException, RetryError):
            return None

    def _get_node_attribute(self, xpath: str, attribute_name: str, parent_element: Optional[WebElement] = None,
                            max_attempts: int = 1) -> Optional[str]:
        try:
            return unify_string(self._get_node(xpath, parent_element, max_attempts).get_attribute(attribute_name))
        except (NoSuchElementException, RetryError):
            return None

    def _get_node(self, xpath: str, parent_element: Optional[WebElement] = None, max_attempts: int = 1) -> WebElement:
        parent = self._chrome_driver if parent_element is None else parent_element
        if max_attempts == 1:  # for backwards compatibility (raises NoSuchElementException instead of RetryError below)
            return parent.find_element_by_xpath(xpath)
        for poll_attempt in self._retry_polling(max_attempts):
            with poll_attempt:
                return parent.find_element_by_xpath(xpath)

    def _get_node_texts(self, xpath: str, parent_element: WebElement) -> List[Optional[str]]:
        return [unify_string(element.text) for element in parent_element.find_elements_by_xpath(xpath)]

    def _extract_media_data_from_browser(self, media_url: str) -> Optional[bytes]:
        if unify_string(media_url) is None:
            return None
        if self._get_main_frame_id() is None:
            raise SnapshotMissingMediaError()
        try:
            response = self._execute_cdp_cmd('Page.getResourceContent',
                                             {'frameId': self._get_main_frame_id(), 'url': media_url})
            resource_content = response['content']
            is_encoded = response['base64Encoded']
        except WebDriverException:
            # resource not found
            log.warning("Could not extract data for resource '%s'", media_url)
            raise SnapshotMissingMediaError()
        result = b64decode(resource_content) if is_encoded else resource_content
        if len(result) == 0 and self._raise_if_missing_image:
            raise SnapshotMissingMediaError()
        return result

    def _get_main_frame_id(self) -> str:
        # the main frame ID appears to remain the same as long as the browser is running
        if self._main_frame_id is None:
            try:
                resource_tree = self._execute_cdp_cmd('Page.getResourceTree', {})
                self._main_frame_id = resource_tree['frameTree']['frame']['id']
                log.debug("The ID of the main frame in Chrome is '%s'", self._main_frame_id)
            except WebDriverException:
                log.warning("Could not extract main frame ID from Chrome", exc_info=True)
        return self._main_frame_id

    def _execute_cdp_cmd(self, cmd: str, cmd_args: dict) -> dict:
        return self._chrome_driver.execute("executeCdpCommand", {'cmd': cmd, 'params': cmd_args})['value']

    def _retry_polling(self, max_attempts: int) -> Retrying:
        return Retrying(before_sleep=before_sleep_log(log, DEBUG),
                        stop=stop_after_attempt(max_attempts),
                        wait=wait_fixed(self._element_poll_interval_seconds))
