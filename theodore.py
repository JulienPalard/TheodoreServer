"""
# Long polling pub-sub server

## Single channel query:

    curl 0:7300/CHANNEL?OPTIONS

With OPTIONS:

 - `min_id=[n|next]`: To select a message by its number.
   special id `next` gives the next one.
 - `no_wait`: This option tells the server not to wait for a new message,
   instead return a 404 in the case it have nothing.

Example:

    curl 0:7300/foo?min_id=next

## Stats

You can request some stats about a channel with this call:

    curl 0:7300/stats?channel=<channel>

## Multi channel query:

    http://0:7300/?channel1=options[&channel2=options[&no_wait]]

HTTP header responses will contain:

 - `X-Id-%s: %u` with `%s` a channel name and `%u` its ID. This header apprear
    once per requested channel.
 - `X-Id`: The id of this message.
 - `X-Channel`: The channel of this message.


"""

import collections
from asyncio import events, Future, wait, wait_for, FIRST_COMPLETED
from aiohttp import web


def date_1123():
    """Get "now" as an RFC 1123 formated string.
    """
    from wsgiref.handlers import format_date_time
    from datetime import datetime
    from time import mktime

    now = datetime.now()
    stamp = mktime(now.timetuple())
    return format_date_time(stamp)


class Message:
    """A single Message in a Channel, see :class:`Channel`.
    """
    def __init__(self, channel_name, msg_id, data):
        self.channel_name = channel_name
        self.msg_id = msg_id
        self.data = data


class Channel:
    """A :class:`Channel` contains messages, each message have an id.
    This class allow querying a message by id, the current message,
    or the next messages.
    """
    def __init__(self, name, loop=None):
        if loop is not None:
            self._loop = loop
        else:
            self._loop = events.get_event_loop()
        self.waiters = collections.deque()
        self.name = name
        self.messages = {}
        self.next_id = 0

    async def get_next(self):
        """Block until the a new message is available.
        """
        fut = Future(loop=self._loop)
        self.waiters.append(fut)
        try:
            return await fut
        finally:
            self.waiters.remove(fut)

    async def get_by_id(self, msg_id):
        if msg_id in self.messages:
            return self.messages[msg_id]
        while True:
            message = await self.get_next()
            if message.msg_id >= msg_id:
                return message

    def _get_next_id(self):
        self.next_id += 1
        return self.next_id

    def push(self, message):
        message = Message(self.name, self._get_next_id(), message)
        self.messages[message.msg_id] = message
        for fut in self.waiters:
            if not fut.done():
                fut.set_result(message)


class TheodoreServer:
    """TheodoreServer dispatches push and pulls of messages to channels.
    """
    def __init__(self, loop=None):
        if loop is not None:
            self._loop = loop
        else:
            self._loop = events.get_event_loop()
        self.channels = {}
        self.start_date = date_1123()

    def get_channel(self, channel_name):
        return self.channels.setdefault(channel_name, Channel(channel_name))

    async def get_multiple(self, channels, timeout=None):
        futures = []
        for channel_name, min_id in channels.items():
            futures.append(self.get(channel_name, min_id))
        done, _ = await wait(futures, timeout=timeout,
                             return_when=FIRST_COMPLETED)
        for task in done:
            return task.result()
        return None

    async def get(self, channel_name, min_id='next', timeout=None):
        """min_id can be either:
         - next to get the next message (will wait for it)
         - None to get the last message (should already have it, if channel
           is not empty)
         - a positive integer to get a message with an id >= to it
         - TODO: a negative integer to get a message relative to the given id
           from the last id.
        """
        channel = self.get_channel(channel_name)
        if min_id == 'next' or min_id == 'NaN':
            future = channel.get_next()
        elif min_id is None:
            future = channel.get_by_id(channel.next_id - 1)
        else:
            future = channel.get_by_id(int(min_id))
        try:
            return await wait_for(future, timeout, loop=self._loop)
        except TimeoutError:
            return None

    def push(self, channel_name, message):
        self.get_channel(channel_name).push(message)


async def get(request):
    """Client pulling one message from one channel.
    It's one of:
     - /channel_name?min_id=next[&no_wait]
     - /channel_name?min_id={id}[&no_wait]
    min_id defaults to channel.id (current message)
    next means channel.id + 1 (future message)
    """
    theodore = request.app['theodore']
    channel_name = request.match_info.get('name', '')
    if channel_name == 'stats':
        return await get_stats(request)
    timeout = None
    if 'no_wait' in request.GET:
        timeout = .1
    if request.GET.get('_http_equiv_x_start_date',
                       theodore.start_date) != theodore.start_date:
        return web.Response(status=412,
                            headers={'X-Start-Date': theodore.start_date})
    min_id = request.GET.get('min_id', None)
    message = await request.app['theodore'].get(
        channel_name, min_id, timeout=timeout)
    if message is None:
        return web.Response(status=404)
    return web.Response(body=message.data,
                        headers={'X-Id': str(message.msg_id)})


async def post(request):
    channel_name = request.match_info.get('name', '')
    request.app['theodore'].push(channel_name, await request.read())
    return web.Response()


async def get_multichannel(request):
    channels = dict(request.GET)
    timeout = None
    if 'no_wait' in channels:
        timeout = .1
        del channels['no_wait']
    if len(channels) == 0:
        return web.Response(status=422)
    message = await request.app['theodore'].get_multiple(
        channels, timeout=timeout)
    if message is None:
        return web.Response(status=404)
    headers = {'X-Id': str(message.msg_id),
               'X-Channel': message.channel_name}
    for channel in channels.keys():
        headers['X-Id-' + channel] = str(
            request.app['theodore'].get_channel(channel).next_id)
    return web.Response(body=message.data,
                        headers=headers)


async def get_stats(request):
    channel = request.app['theodore'].get_channel(
        request.GET.get('channel', ''))
    body = "name: {}\nsubscribers: {}\nmessages: {}\n ".format(
        channel.name, len(channel.waiters), len(channel.messages))
    return web.Response(body=body.encode('utf-8'))


def main():
    app = web.Application()
    app['theodore'] = TheodoreServer()
    app.router.add_route('GET', '/{name}', get)
    app.router.add_route('POST', '/{name}', post)
    app.router.add_route('GET', '/', get_multichannel)
    web.run_app(app, port=7300)


if __name__ == '__main__':
    main()
