import asyncio
import audioop
import wave
import discord
import dotenv
import io

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

	vc.start_recording(
		discord.sinks.WaveSink(),
		once_done,
		ctx.channel,
		vc  # Pass the voice client to the callback
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


async def once_done(sink: discord.sinks, channel: discord.TextChannel, vc: discord.VoiceClient, *args):  # Our voice client already passes these in.
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
		
		# Reset file pointer to the beginning
		audio.file.seek(0)
		
		# Read the entire audio data
		audio_data = audio.file.read()
		file_size = len(audio_data)
		data_size = file_size - 44  # Total size minus header size
		
		# Update file size in header (bytes 4-7)
		audio_data = audio_data[:4] + (file_size - 8).to_bytes(4, 'little') + audio_data[8:]
		
		# Update data chunk size in header (bytes 40-43)
		audio_data = audio_data[:40] + data_size.to_bytes(4, 'little') + audio_data[44:]
		
		# Create a new BytesIO object with the corrected data
		corrected_audio = io.BytesIO(audio_data)
		
		with wave.open(corrected_audio, 'rb') as wav_file:
			n_channels = wav_file.getnchannels()
			sampwidth = wav_file.getsampwidth()
			framerate = wav_file.getframerate()
			n_frames = wav_file.getnframes()
			pcm_data = wav_file.readframes(n_frames)
		audio.file.seek(0)  # reset file pointer to the beginning
		
		# Debug: Log audio parameters
		print(f"User {user_id}: channels={n_channels}, sampwidth={sampwidth}, framerate={framerate}, n_frames={n_frames}")
		print(f"PCM data length: {len(pcm_data)}")

		# If you need to send this corrected audio data to another application:
		# You can use 'audio_data' (bytes) or 'corrected_audio' (BytesIO)

		 # Log audio parameters
		print(f"User {user_id}: channels={n_channels}, sampwidth={sampwidth}, framerate={framerate}, n_frames={n_frames}")
		
		 # Ensure audio is in the correct format for Discord
		if sampwidth != 2 or n_channels != 2 or framerate != 48000:
			print("Converting audio to 16-bit stereo 48kHz")
			pcm_data = audioop.ratecv(pcm_data, sampwidth, n_channels, framerate, 48000, None)[0]
			pcm_data = audioop.lin2lin(pcm_data, sampwidth, 2)
			if n_channels == 1:
				pcm_data = audioop.tostereo(pcm_data, 2, 1, 1)
			sampwidth = 2
			n_channels = 2
			framerate = 48000

		# Calculate frame size (20ms of audio)
		frame_size = int(framerate * 0.02) * n_channels * sampwidth
		print(f"Frame size: {frame_size} bytes")

		print("Audio length:", len(pcm_data)/frame_size * 0.02, "seconds") 
		# Send PCM data in 20ms frames
		for i in range(0, len(pcm_data), frame_size):
			frame = pcm_data[i:i+frame_size]
			# Pad the last frame if necessary
			if len(frame) < frame_size:
				frame += b'\x00' * (frame_size - len(frame))
			vc.send_audio_packet(frame, encode=True)
			await asyncio.sleep(0.02)  # Wait for 20ms
	
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




