#  This file is part of Headphones.
#
#  Headphones is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Headphones is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with Headphones.  If not, see <http://www.gnu.org/licenses/>.
import hashlib

import discogs_client
from Levenshtein import ratio
from discogs_client import Master
from discogs_client.exceptions import HTTPError

import headphones
import headphones.lock
from headphones import db, helpers, logger

try:
    # pylint:disable=E0611
    # ignore this error because we are catching the ImportError
    from collections import OrderedDict
    # pylint:enable=E0611
except ImportError:
    # Python 2.6.x fallback, from libs
    from ordereddict import OrderedDict

discogs_lock = headphones.lock.TimedLock(0)

DISCOGS_CLIENT = None


def start_discogs():
    agent = "headphones/0.0 (\"https://github.com/rembo10/headphones\")"
    global DISCOGS_CLIENT
    discogs_token = headphones.CONFIG.DISCOGS_TOKEN
    DISCOGS_CLIENT = discogs_client.Client(agent, user_token=discogs_token)
    discogs_lock.minimum_delta = 1


def findArtist(name, limit=1):
    artist_list = []
    criteria = {"type": "artist", "limit": limit}

    with discogs_lock:
        artist_results = DISCOGS_CLIENT.search(name, **criteria)
        artist_results._per_page = 100

    current_res = 0
    page_it = (artist_results.page(i) for i in range(1, artist_results.pages + 1))
    while True:  # TODO: create an helper to create iterators with locks
        with discogs_lock:
            try:
                page = next(page_it)
            except StopIteration:
                break
        for artist in page:
            if limit == 1 and artist.name.lower() != name.lower():
                logger.info('Ambiguous artists name: %s - doing an album based search' % name)
                artistdict = findArtistbyAlbum(name)
                if not artistdict:
                    logger.info(
                        'Cannot determine the best match from an artist/album search. Using top match instead')
                    artist_list.append({
                        # Just need the artist id if the limit is 1
                        'id': artist.id,
                    })
                else:
                    artist_list.append(artistdict)
            else:
                score = ratio(artist.name, name) * 100
                image_url = "interfaces/default/images/icon_mic.png"
                if artist.images:
                    image_url = artist.images[0]['uri150']

                artist_list.append({
                    'name': artist.name,
                    'uniquename': artist.name,
                    'id': unicode(artist.id),
                    'url': artist.data['uri'],
                    'image_url': image_url,
                    # probably needs to be changed
                    'score': score
                })
                current_res += 1
    return artist_list


def findRelease(name, limit=1, artist=None):
    releaselist = []

    # additional artist search
    if not artist and ':' in name:
        name, artist = name.rsplit(":", 1)

    name = name.lower()
    query = name
    if artist:
        query += artist.lower()

    results = DISCOGS_CLIENT.search(query, type="release")
    results._per_page = 100
    page_nums = range(1, results.pages + 1)
    release_results = []
    for page_num in page_nums:
        with discogs_lock:
            page = results.page(page_num)
        release_results += [r for r in page if r.status == u"Accepted"]
    release_to_master = dict()
    masters = [r for r in release_results if isinstance(r, Master)]
    for m in masters:
        for version_id in m.data["versions"]:
            release_to_master[version_id] = m
    releases_with_no_master = [r for r in release_results
                               if not isinstance(r, Master) and r.id not in release_to_master]
    for r in releases_with_no_master:
        release_to_master[r.id] = r

    if not release_results:
        logger.debug("Could not find any accepted release for search: '%s'", name)
        return False

    if limit:
        release_results = release_results[:limit]

    for result in release_results:
        formats = []
        for format in result.formats:
            formats.append(u" ".join(format.get("descriptions", [format["name"]])))
        joined_formats = u", ".join(formats)
        release_type = _discogs_formats_to_type(formats)
        image_url = "interfaces/default/images/no-cover-art.png"
        if result.images:
            main_image = result.images[0]
            image_url = main_image["uri150"]
        releaselist.append({
            'uniquename': result.artists[0].name,
            'title': result.title,
            'id': unicode(result.artists[0].id),
            'albumid': unicode(result.id),
            'url': release_to_master[result.id].data.get("uri"),
            'albumurl': result.data.get("uri"),
            'image_url': image_url,
            'score': ratio(name, result.title.lower()) * 100,
            'date': result.data.get("year", u""),
            'country': result.data.get("country", u""),
            'formats': joined_formats,
            'tracks': len(result.tracklist),
            'rgid': unicode(release_to_master[result.id].id),
            'rgtype': release_type
        })
    releaselist = sorted(releaselist, key=lambda x: x["score"], reverse=True)
    return releaselist


def getArtist(artistid, extrasonly=False):
    # The extrasonly flag is ignore since the Discogs API does not seem to have
    # this notion
    logger.info("Retrieving info for '%s' in discogs music database", artistid)
    artist_dict = {}

    with discogs_lock:
        artist = DISCOGS_CLIENT.artist(artistid)

    if not artist:
        logger.info("Cloudn't find artist with ID '%s' on Discogs", artistid)
        return False

    artist_dict['artist_name'] = artist.name

    # For discogs we always include extra
    all_releases = []
    artist.releases._per_page = 100
    pages_it = (artist.releases.page(i) for i in range(1, artist.releases.pages + 1))
    while True:
        with discogs_lock:
            try:
                page = next(pages_it)
            except StopIteration:
                break
        all_releases += page

    # First add main releases of master releases
    displayed_releases = []
    masters = [v for v in all_releases if isinstance(v, Master)]
    for m in masters:
        release_group = {
            "master": m,
            "main_release_id": m.data["main_release"]
        }
        m.versions._per_page = 100
        version_pages_it = (m.versions.page(i) for i in range(1, m.versions.pages + 1))
        while True:
            with discogs_lock:
                try:
                    page = next(version_pages_it)
                    release_group["versions"] = {v.id: v for v in page}
                except StopIteration:
                    break
                displayed_releases.append(release_group)
    # Then add releases that where not in master versions
    seen_versions = set(rid for rg in displayed_releases for rid in rg["versions"])
    releases_with_no_master = [v for v in all_releases if not isinstance(v, Master)
                               and v.id not in seen_versions if v.status == u"Accepted"]
    # For release with no master we take them as their own master and them as the single version
    for release in releases_with_no_master:
        release_group = {
            "master": release,
            "versions": {release.id: release},
            "main_release_id": release.id
        }
        displayed_releases.append(release_group)

    releasegroups = []
    for release in displayed_releases:
        formats = [v.data["format"] for v in release["versions"].values()]
        release_type = _discogs_formats_to_type(formats)
        releasegroups.append({
            'title': release["master"].title,
            'id': unicode(release["master"].id),
            'url': release["master"].data.get('uri'),
            'type': release_type,
            'versions': release["versions"].values(),
            'main_release_id': release["main_release_id"]
        })
    artist_dict['releasegroups'] = releasegroups
    return artist_dict


def getReleaseGroup(rgid):
    """
    Returns a list of releases in a release group
    """
    release_group = None
    try:
        with discogs_lock:
            release_group = DISCOGS_CLIENT.release(rgid)
            master_id = release_group[]
    except HTTPError as e:
        logger.warn(
            'Attempt to retrieve information from MusicBrainz for release group "%s" failed (%s)' % (
                rgid, str(e)))

    if not release_group:
        return False
    else:
        return release_group['release-list']


def getRelease(releaseid, include_release_group_info=True):
    """
    Deep release search to get track info
    """
    release_dict = {}
    release = None

    try:
        with discogs_lock:
            release = DISCOGS_CLIENT.release(releaseid)
    except HTTPError as e:
        logger.warn('Attempt to retrieve information from Discogs for release "%s" failed (%s)'
                    % (releaseid, str(e)))
    if not release:
        return False

    release_dict['title'] = release.title
    release_dict['id'] = unicode(release.id)
    release_dict['date'] = release.data.get("year")
    release_dict['format'] = release.data.get("format")
    release_dict['country'] = release.data.get('country')

    if include_release_group_info:
        if "master_id" in release.data:
            release_dict['rgid'] = unicode(release.data["master_id"])
        else:
            release_dict['rgid'] = unicode(release.id)
        release_dict['rg_title'] = release.title
        formats = []
        for format in release.formats:
            formats.append(u" ".join(format.get("descriptions", [format["name"]])))
        release_dict['rg_type'] = _discogs_formats_to_type(formats)
        release_dict['artist_name'] = release.data["artists"][0]["name"]
        release_dict['artist_id'] = unicode(release.data["artists"][0]["id"])

    release_dict['tracks'] = get_discogs_release_tracks(release)
    return release_dict


def get_new_releases(release_group, includeExtras=False, forcefull=False):
    # Discogs release_group info have already been fetched, no need to call the api again
    myDB = db.DBConnection()
    results = release_group["versions"]
    if not includeExtras or headphones.CONFIG.OFFICIAL_RELEASES_ONLY:
        results = [r for r in results if r.status == "Accepted"]

    if not results or len(results) == 0:
        logger.debug("No release with accepted status")
        logger.debug("%s", [r.status for r in release_group["versions"]])
        return False

    rgid = release_group["id"]
    # Clean all references to releases in dB that are no longer referenced in discogs
    release_list = []
    force_repackage1 = 0
    if len(results) != 0:
        for release in results:
            release_list.append(unicode(release.id))
            release_title = release.title
        remove_missing_releases = myDB.action("SELECT ReleaseID FROM allalbums WHERE AlbumID=?",
                                              [rgid])
        if remove_missing_releases:
            for items in remove_missing_releases:
                if items['ReleaseID'] not in release_list and items['ReleaseID'] != rgid:
                    # Remove all from albums/tracks that aren't in release
                    myDB.action("DELETE FROM albums WHERE ReleaseID=?", [items['ReleaseID']])
                    myDB.action("DELETE FROM tracks WHERE ReleaseID=?", [items['ReleaseID']])
                    myDB.action("DELETE FROM allalbums WHERE ReleaseID=?", [items['ReleaseID']])
                    myDB.action("DELETE FROM alltracks WHERE ReleaseID=?", [items['ReleaseID']])
                    logger.info(
                        "Removing all references to release %s to reflect MusicBrainz" % items[
                            'ReleaseID'])
                    force_repackage1 = 1
    else:
        logger.info(
            "There was either an error pulling data from MusicBrainz or there might not be any releases for this category")

    num_new_releases = 0

    for releasedata in results:


release = {}
rel_id_check = releasedata.id
album_checker = myDB.action('SELECT * from allalbums WHERE ReleaseID=?',
                            [rel_id_check]).fetchone()
if not album_checker or forcefull:
    # DELETE all references to this release since we're updating it anyway.
    myDB.action('DELETE from allalbums WHERE ReleaseID=?', [rel_id_check])
    myDB.action('DELETE from alltracks WHERE ReleaseID=?', [rel_id_check])
    release['AlbumTitle'] = releasedata.title
    release['AlbumID'] = rgid
    release['ReleaseDate'] = releasedata.year if releasedata.year else None
    release['ReleaseID'] = releasedata.id
    release['Type'] = _discogs_formats_to_type([releasedata.data["format"]])

    # making the assumption that the most important artist will be first in the list
    if releasedata.artists:
        release['ArtistID'] = unicode(releasedata.artists[0].id)
        release['ArtistName'] = releasedata.artists[0].name
    else:
        logger.warn('Release ' + releasedata['id'] + ' has no Artists associated.')
        return False

    release['ReleaseCountry'] = releasedata.country if 'country' in releasedata.country else u'Unknown'
    # assuming that the list will contain media and that the format will be consistent
    if releasedata.formats:
        descriptions = [u"(%s)" % u", ".join(format.get('descriptions', [format["name"]])) for format in releasedata.formats]
        release['ReleaseFormat'] = u", ".join(descriptions)
    else:
        release['ReleaseFormat'] = u'Unknown'

    release['Tracks'] = get_discogs_release_tracks(releasedata)

    # What we're doing here now is first updating the allalbums & alltracks table to the most
    # current info, then moving the appropriate release into the album table and its associated
    # tracks into the tracks table
    controlValueDict = {"ReleaseID": release['ReleaseID']}

    newValueDict = {"ArtistID": release['ArtistID'],
                    "ArtistName": release['ArtistName'],
                    "AlbumTitle": release['AlbumTitle'],
                    "AlbumID": release['AlbumID'],
                    "AlbumASIN": None,
                    "ReleaseDate": release['ReleaseDate'],
                    "Type": release['Type'],
                    "ReleaseCountry": release['ReleaseCountry'],
                    "ReleaseFormat": release['ReleaseFormat']
                    }

    myDB.upsert("allalbums", newValueDict, controlValueDict)

    for track in release['Tracks']:

        cleanname = helpers.clean_name(
            release['ArtistName'] + ' ' + release['AlbumTitle'] + ' ' + track['title'])

        controlValueDict = {"TrackID": track['id'],
                            "ReleaseID": release['ReleaseID']}

        newValueDict = {"ArtistID": release['ArtistID'],
                        "ArtistName": release['ArtistName'],
                        "AlbumTitle": release['AlbumTitle'],
                        "AlbumID": release['AlbumID'],
                        "TrackTitle": track['title'],
                        "TrackDuration": track['duration'],
                        "TrackNumber": track['number'],
                        "CleanName": cleanname
                        }

        match = myDB.action('SELECT Location, BitRate, Format from have WHERE CleanName=?',
                            [cleanname]).fetchone()

        if not match:
            match = myDB.action(
                'SELECT Location, BitRate, Format from have WHERE ArtistName LIKE ? AND AlbumTitle LIKE ? AND TrackTitle LIKE ?',
                [release['ArtistName'], release['AlbumTitle'], track['title']]).fetchone()
            # if not match:
            # match = myDB.action('SELECT Location, BitRate, Format from have WHERE TrackID=?', [track['id']]).fetchone()
        if match:
            newValueDict['Location'] = match['Location']
            newValueDict['BitRate'] = match['BitRate']
            newValueDict['Format'] = match['Format']
            # myDB.action('UPDATE have SET Matched="True" WHERE Location=?', [match['Location']])
            myDB.action('UPDATE have SET Matched=? WHERE Location=?',
                        (release['AlbumID'], match['Location']))

        myDB.upsert("alltracks", newValueDict, controlValueDict)
    num_new_releases = num_new_releases + 1
    if album_checker:
        logger.info('[%s] Existing release %s (%s) updated' % (
            release['ArtistName'], release['AlbumTitle'], rel_id_check))
    else:
        logger.info('[%s] New release %s (%s) added' % (
            release['ArtistName'], release['AlbumTitle'], rel_id_check))
if force_repackage1 == 1:
    num_new_releases = -1
    logger.info('[%s] Forcing repackage of %s, since dB releases have been removed' % (
        release['ArtistName'], release_title))
else:
    num_new_releases = num_new_releases

return num_new_releases


def get_discogs_release_tracks(release):
    track_position = 0
    tracks = []
    for track in release.tracklist:
        title = track.data["title"]
        track_id = _build_discogs_track_id(title, release.id)
        track_position += 1
        track_data = {
            'number': track_position,
            'title': title,
            'id': track_id
        }
        if hasattr(track, "duration"):
            track_data["duration"] = _track_duration_from_string(track.duration)

        tracks.append(track_data)
    return tracks


def _build_discogs_track_id(track_title, release_id):
    title_hash = unicode(hashlib.md5(track_title.encode("utf-8")).hexdigest())
    return u"%s-%s" % (release_id, title_hash)


def _track_duration_from_string(string_duration):
    if not string_duration:
        return None
    split = string_duration.split(":")
    length = len(split)
    multipliers = [1, 60, 60 * 60]
    duration = 0
    for (i, m) in zip(range(len(multipliers)), multipliers):
        if length > i:
            duration += int(split[i]) * m
    return duration


# Used when there is a disambiguation


def findArtistbyAlbum(name):
    myDB = db.DBConnection()

    album_title = myDB.action(
        'SELECT AlbumTitle from have WHERE ArtistName=? AND AlbumTitle IS NOT NULL ORDER BY RANDOM()',
        [name]).fetchone()

    if not album_title:
        return False

    # Probably not neccessary but just want to double check
    if not album_title['AlbumTitle']:
        return False

    search = "%s %s" % (album_title, name)
    criteria = {"type": "release"}
    with discogs_lock:
        results = DISCOGS_CLIENT.search(search, criteria)

    if not results:
        return False
    new_artist = results[0].artists[0]
    artist_dict = {
        "id": unicode(new_artist.id)
    }
    return artist_dict


def findAlbumID(artist=None, album=None):
    results = None

    try:
        if album and artist:
            criteria = {'release': album.lower()}
            criteria['artist'] = artist.lower()
        else:
            criteria = {'release': album.lower()}
        with discogs_lock:
            results = musicbrainzngs.search_release_groups(limit=1, **criteria).get(
                'release-group-list')
    except musicbrainzngs.WebServiceError as e:
        logger.warn(
            'Attempt to query MusicBrainz for %s - %s failed (%s)' % (artist, album, str(e)))
        discogs_lock.snooze(5)

    if not results:
        return False

    if len(results) < 1:
        return False
    rgid = unicode(results[0]['id'])
    return rgid


def getArtistForReleaseGroup(rgid):
    """
    Returns artist name for a release group
    Used for series where we store the series instead of the artist
    """
    releaseGroup = None
    try:
        with discogs_lock:
            releaseGroup = musicbrainzngs.get_release_group_by_id(
                rgid, ["artists"])
            releaseGroup = releaseGroup['release-group']
    except musicbrainzngs.WebServiceError as e:
        logger.warn(
            'Attempt to retrieve information from MusicBrainz for release group "%s" failed (%s)' % (
                rgid, str(e)))
        discogs_lock.snooze(5)

    if not releaseGroup:
        return False
    else:
        return releaseGroup['artist-credit'][0]['artist']['name']


DISCOGS_TYPES = [u"Album", u"EP", u"Single", u"Compilation"]


def _discogs_formats_to_type(formats):
    release_type = None
    for type in DISCOGS_TYPES:
        for format in formats:
            if type.lower() in format.lower():
                release_type = type
                break
        if release_type is not None:
            break

    if release_type is None:
        release_type = u"Other"
    return release_type


def build_hybrid_release(rg, _):
    # In the case of discogs it's the master main release

    # 1. Get the main release id
    # 2; Get the
    version_dict = {v.id: v for v in rg["versions"]}
    main_release = version_dict[rg["main_release_id"]]
    tracks = get_discogs_release_tracks(main_release)
    return {'ReleaseDate': main_release.data.get("year"), 'Tracks': tracks}


def get_single_release_from_album(album_id):
    return getRelease(album_id, include_release_group_info=True)
