import asyncio
import audioop
import time
import wave
import discord
import dotenv
import io
from openai_realtime_client import RealtimeClient, InputHandler, AudioHandler

bot = discord.Bot()
connections = {}

@bot.command()
async def record(ctx):
	voice = ctx.author.voice

	if not voice:
		await ctx.respond("You aren't in a voice channel!")
		return

	vc = await voice.channel.connect()
	connections.update({ctx.guild.id: vc})
	chat_client = init_realtime_client()

	vc.start_recording(
		discord.sinks.WaveSink(),
		once_done,
		ctx.channel,
		vc,  # Pass the voice client to the callback
		chat_client
	)
	await ctx.respond("Started recording!")


def fix_audio(audio_file):
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


def init_realtime_client():
	client = RealtimeClient(
		api_key=dotenv.get_key('.env','OPENAI_API_KEY'),
		on_text_delta=lambda text: print(f"\nAssistant: {text}", end="", flush=True),
		on_audio_delta=lambda audio: audio_cbk(audio),
		tools=[],
	)
	return client


def audio_cbk(audio):
	print(len(audio))
	# audio_handler.play_audio(audio) 
	openai_samplerate = 24000
	openai_sampwidth = 2
	openai_n_channels = 1
	# await play_audio(vc, audio, openai_samplerate, openai_sampwidth, openai_n_channels)


async def play_audio(vc: discord.VoiceClient, pcm_data, sampwidth, n_channels, framerate):
	# plays audio in a discord voice channel. converts input audio to PCM first.
	pcm_data, sampwidth, n_channels, framerate = to_discord_format(pcm_data, sampwidth, n_channels, framerate)
	frame_size = int(framerate * 0.02) * n_channels * sampwidth  # discord takes 20ms frames
	print("Audio length:", len(pcm_data)/frame_size * 0.02, "seconds") 
	for i in range(0, len(pcm_data), frame_size):
		start_time = time.time()
		frame = pcm_data[i:i+frame_size]
		# Pad the last frame if necessary
		if len(frame) < frame_size:
			frame += b'\x00' * (frame_size - len(frame))
		vc.send_audio_packet(frame, encode=True)
		work_time = time.time() - start_time
		sleep_time = max(0, 0.02 - work_time)
		await asyncio.sleep(sleep_time)  # Wait for 20ms


async def once_done(sink: discord.sinks, channel: discord.TextChannel, vc: discord.VoiceClient, chat_client: RealtimeClient, *args): 
	recorded_users = [  # A list of recorded users
		f"<@{user_id}>"
		for user_id, audio in sink.audio_data.items()
	]
	
	files = []
	print(sink.audio_data)
	print(sink.encoding)

	for user_id, audio in sink.audio_data.items():
		files.append(discord.File(audio.file, f"{user_id}.{sink.encoding}"))
		sink.format_audio(audio)
		
		corrected_audio, audio_data = fix_audio(audio.file)

		# Get language model response TODO
		# await chat_client.send_audio(audio_data)
		
		with wave.open(corrected_audio, 'rb') as wav_file:
			n_channels = wav_file.getnchannels()
			sampwidth = wav_file.getsampwidth()
			framerate = wav_file.getframerate()
			n_frames = wav_file.getnframes()
			pcm_data = wav_file.readframes(n_frames)
		audio.file.seek(0)  # reset file pointer to the beginning

		await play_audio(vc, pcm_data, sampwidth, n_channels, framerate)
	
	await sink.vc.disconnect()  # Disconnect from the voice channel.


	await channel.send(f"finished recording audio for: {', '.join(recorded_users)}.", files=files)  # Send a message with the accumulated files.

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




