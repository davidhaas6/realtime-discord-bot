# Discord speech to speech bot
"""
Ideas
 - remove sleep statement from audio -- seems to do worse when assistant has long replies
 - Tool use
   - leave server
   - send message
 - interruptions
 - Prompt to not chime in too often -- it's not the center of the conversation. just chime in with "mhm" unless its directly addressed. ask if you're not sure if you're being addressed.
 - server-specific memory
 - somehow convey the usernames to the bot
"""

import asyncio
from dataclasses import dataclass
import wave
from io import BytesIO
from typing import Callable

import discord
import dotenv
import io

from mixing import main_mix_function
from openai_realtime_client import RealtimeClient, TurnDetectionMode
from discord_audio_handler import DiscordAudioHandler
import time
from llama_index.core.tools import FunctionTool
import nest_asyncio
import asyncio

nest_asyncio.apply()

bot = discord.Bot()
connection_data = {}
audio_handler = DiscordAudioHandler()
start_time = time.time()


@dataclass
class ServerContext:
    guild_id: int
    vc: discord.VoiceClient
    ai_client: RealtimeClient
    msg_handler: asyncio.Task
    input_audio_sink: discord.sinks.Sink
    send_message: Callable
    audio_task: asyncio.Task = None  # Store task for continuous audio processing


@bot.command()
async def join(ctx: discord.ApplicationContext):
    voice = ctx.author.voice
    guild_id = ctx.guild.id
    if not voice:
        await ctx.respond("User isn't in a voice channel")
        return

    connection_context = await start_chatting(ctx)
    connection_data.update({guild_id: connection_context})
    print(f'Joined voice channel {voice.channel} - guild {guild_id}')


@bot.command()
async def leave(ctx: discord.ApplicationContext):
    print(f'Leaving voice channel {ctx.channel} - guild {ctx.guild.id}')
    guild_id = ctx.guild.id

    if guild_id in connection_data:
        connection = connection_data[guild_id]
        await cleanup_connection(connection)
    else:
        print(connection_data)
        await ctx.respond("I am not in a channel.")


async def start_chatting(ctx) -> ServerContext:
    channel = ctx.author.voice.channel
    vc = await channel.connect()

    ai_client, msg_handler = await start_realtime(ctx, vc)

    input_audio_sink = discord.sinks.WaveSink()
    vc.start_recording(
        input_audio_sink,
        stop_record_callback,
        channel,
        sync_start=True,
    )

    audio_task = asyncio.create_task(
        continuous_audio_processing(vc, ai_client, input_audio_sink),
        name="continuous_audio_processing"
    )

    return ServerContext(channel.guild.id, vc, ai_client, msg_handler, input_audio_sink, ctx.respond, audio_task)


async def cleanup_connection(connection: ServerContext):
    print("Trying to leave server:", connection)
    if connection.vc.recording:
        connection.vc.stop_recording()
    await connection.ai_client.close()
    await connection.vc.disconnect()
    connection.msg_handler.cancel()
    if connection.audio_task and not connection.audio_task.done():
        connection.audio_task.cancel()
    del connection_data[connection.guild_id]
    print("Left server!")


async def continuous_audio_processing(vc: discord.VoiceClient, ai_client: RealtimeClient,
                                      audio_sink: discord.sinks.Sink):
    """
    Continuously processes audio data from the sink and sends it to the AI client.
    """
    user_map = {}
    can_send_empty = False  # flag to send empty array to api

    while not vc.is_connected():
        await asyncio.sleep(0.1)
    print('Connected!')

    # audio parameters
    fs = vc.decoder.SAMPLING_RATE
    sample_width = vc.decoder.SAMPLE_SIZE // vc.decoder.CHANNELS
    num_channels = vc.decoder.CHANNELS
    frames_needed = int(fs * 0.1)  # 100ms worth of frames
    bytes_per_frame = num_channels * sample_width
    min_bytes = frames_needed * bytes_per_frame * 5

    empty_100ms = io.BytesIO()
    with wave.open(empty_100ms, "wb") as f:
        f.setnchannels(num_channels)
        f.setsampwidth(sample_width)
        f.setframerate(fs)
        f.setnframes(min_bytes)
        f.writeframes(b'\x00' * min_bytes)

    while vc.is_connected():
        await asyncio.sleep(0.5)  # Adjust this interval as needed for responsiveness

        audio_streams = []
        for user_id, audio in list(audio_sink.audio_data.items()):
            if user_id not in user_map:
                user_map[user_id] = await vc.client.fetch_user(user_id)
            discord_user = user_map[user_id]
            if discord_user.bot:
                print('sponge detected -- user', discord_user.name)
                continue
            # print('name', discord_user.name)
            if audio.file.tell() == 0:
                continue

            buffer = io.BytesIO()
            with wave.open(buffer, "wb") as f:
                f.setnchannels(num_channels)
                f.setsampwidth(sample_width)
                f.setframerate(fs)

                audio.file.seek(0)
                audio_bytes = audio.file.read()

                if len(audio_bytes) < min_bytes:
                    audio_bytes += b'\x00' * (min_bytes - len(audio_bytes))

                total_frames = len(audio_bytes) // bytes_per_frame
                f.setnframes(total_frames)
                f.writeframes(audio_bytes)

            buffer.seek(0)
            audio_streams.append(buffer)
            can_send_empty = True

            # Clear buffer after processing to avoid duplication
            audio.file.seek(0)  # is this necessary
            audio.file.truncate(0)
            audio.file.seek(0)

        # get final audio stream
        if len(audio_streams) == 0:
            if can_send_empty:
                mixed_audio_stream = empty_100ms
                can_send_empty = False
            else:
                continue
        else:
            mixed_audio_stream = main_mix_function(
                audio_streams,
                frame_rate=fs,
                target_rms=0.4,
            )

        # query
        try:
            await ai_client.send_audio(mixed_audio_stream.getvalue())
        except Exception as e:
            print(f"Error sending audio: {e}")
    print("Bot disconnected from voice channel; stopping continuous processing.")


def leave_channel():
    """ Leave the Discord voice channel """
    if len(connection_data) == 1:
        connection = list(connection_data.values())[0]
        bot.loop.create_task(cleanup_connection(connection))
        return 'server exited'
    else:
        return 'David hasnt implemented this yet'


def set_system_prompt(prompt_text):
    """ Sets the system prompt for the AI chatbot. Use this when the user asks you to  """
    if len(connection_data) == 1:
        connection = list(connection_data.values())[0]
        bot.loop.create_task(connection.ai_client.update_session({"instructions": prompt_text}))
        return 'updated'
    else:
        return 'David hasnt implemented this yet'


async def start_realtime(ctx: discord.ApplicationContext, vc: discord.VoiceClient):
    def wrapped_audio_cbk(audio):
        audio_callback(audio, vc)
    guild_id = ctx.guild.id

    bot_tools = [FunctionTool.from_defaults(fn=leave_channel)]
    with open('prompts/nature', 'r') as f:
        prompt = f.read().strip()
    client = RealtimeClient(
        api_key=dotenv.get_key('.env', 'OPENAI_API_KEY'),
        on_text_delta=lambda text: print(f"\nAssistant: {text}", end="", flush=True),
        on_audio_delta=wrapped_audio_cbk,
        on_interrupt=on_interrupt,
        turn_detection_mode=TurnDetectionMode.SERVER_VAD,
        voice='alloy',
        tools=bot_tools,
        instructions=prompt
    )
    await client.connect()
    listen_task = asyncio.create_task(client.handle_messages())

    return client, listen_task


def audio_callback(audio: bytes, vc: discord.VoiceClient):
    """
    Play audio response from AI in the voice channel.
    """
    audio_handler.vc = vc
    if vc.is_connected():
        audio_handler.play_audio(audio)


def on_interrupt():
    pass

async def stop_record_callback(sink: discord.sinks, channel: discord.TextChannel):
    pass

bot.run(dotenv.get_key('.env', 'BOT_TOKEN'))
