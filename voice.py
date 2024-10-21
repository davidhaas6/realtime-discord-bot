import asyncio
import audioop
import pickle
import time
import wave
import discord
import dotenv
import io
from openai_realtime_client import RealtimeClient, InputHandler, AudioHandler
import logging

from discord_audio_handler import DiscordAudioHandler

# Update the logger configuration
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Add a console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create a formatter and add it to the handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(console_handler)

bot = discord.Bot()
connections = {}
realtime_clients = {}
response_data = []
audio_handler = DiscordAudioHandler()

@bot.command()
async def record(ctx):
	voice = ctx.author.voice

	if not voice:
		await ctx.respond("You aren't in a voice channel!")
		return
	guild_id = ctx.guild.id

	vc = await voice.channel.connect()
	connections.update({guild_id: vc})

	# chat_client = await init_realtime_client(vc, guild_id)
	# realtime_clients.update({guild_id: chat_client})

	vc.start_recording(
		discord.sinks.WaveSink(),
		record_callback,
		ctx.channel,
		vc,
	)
	await ctx.respond("Started recording!")
	



def update_header(audio_file):
	audio_file.seek(0)
	audio_data = audio_file.read()
	file_size = len(audio_data)
	data_size = file_size - 44  # Total size minus header size
	
	# Update file size in header (bytes 4-7)
	audio_data = audio_data[:4] + (file_size - 8).to_bytes(4, 'little') + audio_data[8:]
	
	# Update data chunk size in header (bytes 40-43)
	audio_data = audio_data[:40] + data_size.to_bytes(4, 'little') + audio_data[44:]
	
	# Create a new BytesIO object with the corrected data
	corrected_audio = io.BytesIO(audio_data)
	audio_file.seek(0)
	return corrected_audio, audio_data


def to_discord_format(pcm_data, sampwidth, n_channels, framerate):
	# convert to 16bit stereo 48khz
	if sampwidth != 2 or n_channels != 2 or framerate != 48000:
		print("Converting audio to 16-bit stereo 48kHz")
		pcm_data = audioop.ratecv(pcm_data, sampwidth, n_channels, framerate, 48000, None)[0]
		pcm_data = audioop.lin2lin(pcm_data, sampwidth, 2)
		if n_channels == 1:
			pcm_data = audioop.tostereo(pcm_data, 2, 1, 1)
		sampwidth = 2
		n_channels = 2
		framerate = 48000
	return pcm_data, sampwidth, n_channels, framerate


def to_realtime_format(pcm_data, sampwidth, n_channels, framerate):
	# convert to 16bit mono 24khz
	if sampwidth != 2 or n_channels != 1 or framerate != 24000:
		print("Converting audio to 16-bit mono 24kHz")
		pcm_data = audioop.ratecv(pcm_data, sampwidth, n_channels, framerate, 24000, None)[0]
		pcm_data = audioop.lin2lin(pcm_data, sampwidth, 2)
		if n_channels == 2:
			pcm_data = audioop.tomono(pcm_data, 2, 1, 1)
		n_channels = 1
		framerate = 24000
		sampwidth = 2
	return pcm_data, sampwidth, n_channels, framerate


def init_realtime_client(vc, guild_id):
	def wrapped_audio_cbk(audio):
		audio_callback(audio, vc)
	
	client = RealtimeClient(
		api_key=dotenv.get_key('.env','OPENAI_API_KEY'),
		on_text_delta=lambda text: print(f"\nAssistant: {text}", end="", flush=True),
		on_audio_delta=wrapped_audio_cbk,
	)
	realtime_clients.update({guild_id: client})
	return client


def audio_callback(audio: bytes, vc):
	audio_handler.vc = vc
	audio_handler.play_audio(audio)


def play_audio(vc: discord.VoiceClient, pcm_data, sampwidth, n_channels, framerate):
	# plays audio in a discord voice channel. converts input audio to PCM first.
	pcm_data, sampwidth, n_channels, framerate = to_discord_format(pcm_data, sampwidth, n_channels, framerate)
	frame_size = int(framerate * 0.02) * n_channels * sampwidth  # discord takes 20ms frames
	
	audio_len_s = len(pcm_data)/frame_size * 0.02
	logger.info(f"Playing audio of length: {audio_len_s} seconds")

	for i in range(0, len(pcm_data), frame_size):
		start_time = time.time()
		frame = pcm_data[i:i+frame_size]
		# Pad the last frame if necessary
		if len(frame) < frame_size:
			frame += b'\x00' * (frame_size - len(frame))
		vc.send_audio_packet(frame, encode=True)

		work_time = time.time() - start_time
		sleep_time = max(0, 0.02 - work_time)
		# await asyncio.sleep(sleep_time)  # Wait for 20ms


async def record_callback(sink: discord.sinks, channel: discord.TextChannel, vc: discord.VoiceClient, *args): 
	logger.info(f"Received audio from users")
	chat_client = init_realtime_client(vc, 123)
	await chat_client.connect()
	asyncio.create_task(chat_client.handle_messages())
	# import pdb; pdb.set_trace()
	logger.info(f"Connected to OpenAI RealTime API!")

	files = []
	for user_id, audio in sink.audio_data.items():
		files.append(discord.File(audio.file, f"{user_id}.{sink.encoding}"))
		sink.format_audio(audio)
		_, audio_data = update_header(audio.file)
		await chat_client.send_audio(audio_data)
		logger.info("Sent audio to OpenAI RealTime API")
		
	await channel.send(f"finished recording audio", files=files)  # Send a message with the accumulated files.

	await asyncio.wait_for(asyncio.sleep(10), timeout=30.0)  # Simulating waiting for a response

	if len(response_data) > 0:
		response = b''.join(response_data)
		AudioHandler().play_audio(response)
	
	await sink.vc.disconnect()  # Disconnect from the voice channel.
	logger.info("Closing connection to OpenAI RealTime API")
	await chat_client.close()  # TODO: move this eventually to have multi-turn conversations
	logger.info("Closed connection to OpenAI RealTime API")

@bot.command()
async def stop_recording(ctx):
	if ctx.guild.id in connections:  # Check if the guild is in the cache.
		vc = connections[ctx.guild.id]
		vc.stop_recording()  # Stop recording, and call the callback (once_done).
		del connections[ctx.guild.id]  # Remove the guild from the cache.
		await ctx.delete()  # And delete.
	else:
		await ctx.respond("I am currently not recording here.")  # Respond with this if we aren't recording.
	



bot.run(dotenv.get_key('.env','BOT_TOKEN'))




