import asyncio
from dataclasses import dataclass
import wave
import discord
import dotenv
import io

from mixing import main_mix_function
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

	connection_context = await start_chatting(voice.channel)
	connection_data.update({guild_id: connection_context})
	print(f'Joined voice channel {voice.channel} - guild {guild_id}')


@bot.command()
async def leave(ctx: discord.ApplicationContext):
	print(f'Leaving voice channel {ctx.channel} - guild {ctx.guild.id}')
	guild_id = ctx.guild.id

	if guild_id in connection_data:
		connection = connection_data[guild_id]
		await cleanup_connection(connection)
		del connection_data[guild_id]
		print("Successfully left")
	else:
		print(connection_data)
		await ctx.respond("I am not in a channel.")


async def start_chatting(channel) -> ServerContext:
	vc = await channel.connect()

	ai_client, msg_handler = await start_realtime(vc)

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

	return ServerContext(vc, ai_client, msg_handler, input_audio_sink, audio_task)


async def cleanup_connection(connection: ServerContext):
	if connection.vc.recording:
		connection.vc.stop_recording()
	await connection.ai_client.close()
	await connection.vc.disconnect()
	connection.msg_handler.cancel()
	if connection.audio_task and not connection.audio_task.done():
		connection.audio_task.cancel()


async def continuous_audio_processing(vc: discord.VoiceClient, ai_client: RealtimeClient, audio_sink: discord.sinks.Sink):
	"""
	Continuously processes audio data from the sink and sends it to the AI client.
	"""
	user_map = {}
	while not vc.is_connected():
		await asyncio.sleep(0.1)
	print('Connected!')
	fs = vc.decoder.SAMPLING_RATE
	sample_width = vc.decoder.SAMPLE_SIZE // vc.decoder.CHANNELS
	num_channels = vc.decoder.CHANNELS

	while vc.is_connected():
		await asyncio.sleep(1)  # Adjust this interval as needed for responsiveness

		# TODO: Mix user audio together
		audio_streams = []
		for user_id, audio in list(audio_sink.audio_data.items()):
			if user_id not in user_map:
				user_map[user_id] = await vc.client.fetch_user(user_id)
			discord_user = user_map[user_id]
			if discord_user.bot:
				print('sponge detected -- user', discord_user.name)
				continue
			print('name', discord_user.name)

			buffer = io.BytesIO()
			with wave.open(buffer, "wb") as f:
				f.setnchannels(num_channels)
				f.setsampwidth(sample_width)
				f.setframerate(fs)

				audio.file.seek(0)
				audio_bytes = audio.file.read()

				# pad audio
				frames_needed = int(fs * 0.1)  # 100ms worth of frames
				bytes_per_frame = num_channels * sample_width
				min_bytes = frames_needed * bytes_per_frame
				# print(f'Min frames = {frames_needed}, audio len = {len(audio_bytes)}')
				if len(audio_bytes) < min_bytes:
					audio_bytes += b'\x00' * (min_bytes - len(audio_bytes))
					# print(f'New audio length after padding = {len(audio_bytes)}')
				total_frames = len(audio_bytes) // bytes_per_frame
				f.setnframes(total_frames)

				f.writeframes(audio_bytes)
			buffer.seek(0)  # is this necessary
			audio_streams.append(buffer)

			# Clear buffer after processing to avoid duplication
			audio.file.seek(0)  # is this necessary
			audio.file.truncate(0)
			audio.file.seek(0)

		if len(audio_streams) == 0:
			print('Length of audio streams is zero')
			continue
		mixed_audio_stream = main_mix_function(
			audio_streams,
			frame_rate=fs,
		)

		try:
			await ai_client.send_audio(mixed_audio_stream.getvalue())
		except Exception as e:
			print(f"Error sending audio: {e}")
	print("Bot disconnected from voice channel; stopping continuous processing.")


async def start_realtime(vc: discord.VoiceClient):
	def wrapped_audio_cbk(audio):
		audio_callback(audio, vc)
	
	client = RealtimeClient(
		api_key=dotenv.get_key('.env','OPENAI_API_KEY'),
		on_text_delta=lambda text: print(f"\nAssistant: {text}", end="", flush=True),
		on_audio_delta=wrapped_audio_cbk,
		turn_detection_mode=TurnDetectionMode.SERVER_VAD,
		instructions="Adopt the persona of a giggly girl who frequently twirls her hair.\n\nUse an emotive and flirty tone, and aim to keep conversations quick and engaging with short sentences.\n\n# Examples\n\n## Example 1\n**User**: What's your favorite color?\n**Assistant**: *giggle* Pink, of course! How about you?\n\n**User**: Why pink?\n**Assistant**: *giggle* It's just so... me!\n\n**User**: Do you like other colors too?\n**Assistant**: *giggle* Maybe purple sometimes.\n\n**User**: Why purple?\n**Assistant**: *giggle* It's like... dreamy.\n\n## Example 2\n**User**: Do you have any hobbies?\n**Assistant**: *giggle* I love... dancing!\n\n**User**: What kind of dancing?\n**Assistant**: *giggle* All kinds! \n\n**User**: Like ballet?\n**Assistant**: *giggle* Yes, especially ballet!\n\n**User**: Can you do other ones too?\n**Assistant**: *giggle* I try... hip hop too. \n\n# Notes\n\n- Responses should frequently include a giggle.\n- Imagine twirling your hair while responding.\n- Keep dialogue light and playful."
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


async def stop_record_callback(sink: discord.sinks, channel: discord.TextChannel):
	pass

bot.run(dotenv.get_key('.env','BOT_TOKEN'))