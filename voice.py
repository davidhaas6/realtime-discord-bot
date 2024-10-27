import asyncio
import audioop
from copy import deepcopy
from dataclasses import dataclass
import pickle
import wave
import discord
import dotenv
import io
import numpy as np
from openai_realtime_client import RealtimeClient, TurnDetectionMode
from discord_audio_handler import DiscordAudioHandler
import time

bot = discord.Bot()
connection_data = {}
audio_handler = DiscordAudioHandler()
start_time = time.time()

@dataclass
class ServerContext:
	vc: discord.VoiceClient
	ai_client: RealtimeClient
	msg_handler: asyncio.Task
	input_audio_sink: discord.sinks.Sink
	audio_task: asyncio.Task = None  # Store task for continuous audio processing


@bot.command()
async def join(ctx: discord.ApplicationContext):
	voice = ctx.author.voice
	guild_id = ctx.guild.id
	if not voice:
		await ctx.respond("User isn't in a voice channel")
		return
	
	vc = await voice.channel.connect()

	ai_client, msg_handler = await start_realtime(vc)	

	input_audio_sink = discord.sinks.WaveSink()
	vc.start_recording(
		input_audio_sink,
		stop_record_callback,
		ctx.channel,
		sync_start=True,
	)
	
	audio_task = asyncio.create_task(continuous_audio_processing(vc, ai_client, input_audio_sink), name="continuous_audio_processing")
	
	server_context = ServerContext(vc, ai_client, msg_handler, input_audio_sink, audio_task)
	connection_data.update({guild_id: server_context})
	print(f'Joined voice channel {voice.channel} - guild {guild_id}')


@bot.command()
async def leave(ctx: discord.ApplicationContext):
	print(f'Leaving voice channel {ctx.channel} - guild {ctx.guild.id}')
	guild_id = ctx.guild.id
	connection = connection_data.get(guild_id, None)
	
	if connection:
		if connection.vc.recording:
			connection.vc.stop_recording()
		await connection.ai_client.close()
		await connection.vc.disconnect()
		connection.msg_handler.cancel()
		if connection.audio_task and not connection.audio_task.done():
			connection.audio_task.cancel()
		del connection_data[guild_id]
		print("Successfully left")
	else:
		print(connection_data)
		await ctx.respond("I am currently not recording here.")


async def continuous_audio_processing(vc: discord.VoiceClient, ai_client: RealtimeClient, audio_sink: discord.sinks.Sink):
	"""
	Continuously processes audio data from the sink and sends it to the AI client.
	"""
	user_map = {}
	while not vc.is_connected():
		await asyncio.sleep(0.2)
	print('connected!')
	while vc.is_connected():
		await asyncio.sleep(1)  # Adjust this interval as needed for responsiveness
		for user_id, audio in list(audio_sink.audio_data.items()):
			if True:
				if user_id not in user_map:
					user_map[user_id] = await vc.client.fetch_user(user_id)
				discord_user = user_map[user_id]
				if 'davinki' not in discord_user.name.lower():
					continue

				buffer = io.BytesIO()
				audio.file.seek(0)
				with wave.open(buffer, "wb") as f:
					fs = vc.decoder.SAMPLING_RATE
					sample_width = vc.decoder.SAMPLE_SIZE // vc.decoder.CHANNELS
					num_channels = vc.decoder.CHANNELS
					f.setnchannels(num_channels)
					f.setsampwidth(sample_width)
					f.setframerate(fs)

					audio_bytes = audio.file.read()
					frames_needed = int(fs * 0.1)  # 100ms worth of frames
					bytes_per_frame = num_channels * sample_width
					min_bytes = frames_needed * bytes_per_frame
					# print(f'Min frames = {frames_needed}, audio len = {len(audio_bytes)}')

					if len(audio_bytes) < min_bytes:
						# Pad the audio bytes to reach the minimum length
						audio_bytes += b'\x00' * (min_bytes - len(audio_bytes))
						# print(f'New audio length after padding = {len(audio_bytes)}')
					
					total_frames = len(audio_bytes) // bytes_per_frame
					f.setnframes(total_frames)
						
					f.writeframes(audio_bytes)
				buffer.seek(0)
				try:
					await ai_client.send_audio(buffer.getvalue())
				except Exception as e:
					print(f"Error sending audio: {e}")
				
				# Clear buffer after processing to avoid duplication
				audio.file.seek(0)  # Reset for the next chunk
				audio.file.truncate(0)
				audio.file.seek(0)
	print("Bot disconnected from voice channel; stopping continuous processing.")


async def start_realtime(vc: discord.VoiceClient):
	def wrapped_audio_cbk(audio):
		audio_callback(audio, vc)
	
	client = RealtimeClient(
		api_key=dotenv.get_key('.env','OPENAI_API_KEY'),
		on_text_delta=lambda text: print(f"\nAssistant: {text}", end="", flush=True),
		on_audio_delta=wrapped_audio_cbk,
		turn_detection_mode=TurnDetectionMode.SERVER_VAD,
		# instructions="You are a concise AI assistant. Respond to the user's question in less than 5 words."
	)
	await client.connect()
	task = asyncio.create_task(client.handle_messages())

	return client, task


def audio_callback(audio: bytes, vc: discord.VoiceClient):
	"""
	Play audio response from AI in the voice channel.
	"""
	audio_handler.vc = vc
	if vc.is_connected():
		audio_handler.play_audio(audio)

def update_header(audio_file: io.BytesIO):
	audio_file.seek(0)
	audio_data = audio_file.read()
	
	# Update file size (bytes 4-7) and chunk size in the header (bytes 40-43)
	file_size = len(audio_data)
	data_size = file_size - 44  # Total size minus header size
	audio_data = audio_data[:4] + (file_size - 8).to_bytes(4, 'little') + audio_data[8:]
	audio_data = audio_data[:40] + data_size.to_bytes(4, 'little') + audio_data[44:]
	
	corrected_audio = io.BytesIO(audio_data)
	audio_file.seek(0)
	return corrected_audio, audio_data

async def stop_record_callback(sink: discord.sinks, channel: discord.TextChannel): 
	pass

bot.run(dotenv.get_key('.env','BOT_TOKEN'))