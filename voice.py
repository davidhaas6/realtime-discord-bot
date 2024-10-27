import asyncio
import audioop
from copy import deepcopy
from dataclasses import dataclass
import discord
import dotenv
import io
import numpy as np
from openai_realtime_client import RealtimeClient
from discord_audio_handler import DiscordAudioHandler


bot = discord.Bot()
connection_data = {}
audio_handler = DiscordAudioHandler()


@dataclass
class ServerContext:
	vc: discord.VoiceClient
	ai_client: RealtimeClient
	msg_handler: asyncio.Task
	input_audio_sink: discord.sinks.Sink


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
		vc,
		ai_client,
		guild_id,
		sync_start=True,
	)

	server_context = ServerContext(vc, ai_client, msg_handler, input_audio_sink)
	connection_data.update({guild_id: server_context})
	# await ctx.respond("Started recording!")

	# start a timer to read from the sink every 0.25 seconds
	# asyncio.create_task(read_from_sink(server_context), name="read_from_sink")


def start_recording(ctx: discord.ApplicationContext):
	guild_id = ctx.guild.id
	if guild_id not in connection_data:
		return
	server_context = connection_data[guild_id]
	server_context.input_audio_sink = discord.sinks.WaveSink() 
	server_context.vc.start_recording(
		server_context.input_audio_sink,
		stop_record_callback,
		ctx.channel,
		server_context.vc,
		server_context.ai_client,
		guild_id,
		sync_start=True,
	)


@bot.command()
async def leave(ctx: discord.ApplicationContext):
	connection = connection_data.get(ctx.guild.id, None)
	if connection: 
		if connection.vc.recording:
			connection.vc.stop_recording()
		await connection.vc.disconnect()
		await connection.ai_client.close()
		
		# cancel all tasks
		connection.msg_handler.cancel()
		# cancel the read_from_sink task
		tasks = asyncio.all_tasks()
		for task in tasks:
			if task.get_name() == "read_from_sink":
				task.cancel()

		del connection_data[ctx.guild.id]
		await ctx.delete() 
	else:
		await ctx.respond("I am currently not recording here.")  


@bot.command()
async def send(ctx: discord.ApplicationContext):
	guild_id = ctx.guild.id
	connection = connection_data.get(guild_id, None)
	if not connection:
		await ctx.respond("I am currently not recording here.")
		return
	
	if connection.vc.recording:
		connection.vc.stop_recording()


@bot.command()
async def listen(ctx: discord.ApplicationContext):
	guild_id = ctx.guild.id
	if guild_id not in connection_data:
		await ctx.respond("I am currently not recording here.")
		return
	if not connection_data[guild_id].vc.recording:
		start_recording(ctx)


async def start_realtime(vc: discord.VoiceClient):
	def wrapped_audio_cbk(audio):
		audio_callback(audio, vc)
	
	client = RealtimeClient(
		api_key=dotenv.get_key('.env','OPENAI_API_KEY'),
		on_text_delta=lambda text: print(f"\nAssistant: {text}", end="", flush=True),
		on_audio_delta=wrapped_audio_cbk,
		on_done=done_callback,
		instructions="You are a concise AI assistant. Respond to the user's question in less than 5 words. Your answer should be highly expressive and dramatic."
		# instructions="You are a helpful AI assistant with an operatic flair. You ♪ SING LOOOOUDLY ♪  whenever you talk or perform a task as you always wish you were performing in the OPERAAAAAAAA…. ♪♪ "
	)
	await client.connect()
	task = asyncio.create_task(client.handle_messages())

	return client, task


def audio_callback(audio: bytes, vc: discord.VoiceClient):
	audio_handler.vc = vc
	audio_handler.play_audio(audio)


def done_callback(event):
	print("Response done")
	print(event)


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


async def stop_record_callback(sink: discord.sinks, channel: discord.TextChannel, vc: discord.VoiceClient, chat_client: RealtimeClient, guild_id: int, *args): 
	files = []
	for user_id, audio in list(sink.audio_data.items()):
		discord_user = await vc.client.fetch_user(user_id)
		if 'davinki' not in discord_user.name.lower():
			continue
		
		print(f"User {discord_user} has sent audio")

		files.append(discord.File(audio.file, f"{user_id}.{sink.encoding}"))

		sink.format_audio(audio)
		_, audio_data = update_header(audio.file)
		await chat_client.send_audio(audio_data)


# async def read_from_sink(server_context: ServerContext):
# 	audio_sink: discord.sinks.Sink = server_context.input_audio_sink
# 	vc = server_context.vc
# 	while True:
# 		await asyncio.sleep(0.5)
# 		for user_id, audio in list(audio_sink.audio_data.items()):
# 			data = deepcopy(audio.file)
# 			with wave.open(data, "wb") as f:
# 				f.setnchannels(vc.decoder.CHANNELS)
# 				f.setsampwidth(vc.decoder.SAMPLE_SIZE // vc.decoder.CHANNELS)
# 				f.setframerate(vc.decoder.SAMPLING_RATE)
# 			# _, audio_data = update_header(audio.file)
# 			print(f"User {user_id} audio data length: {len(data.read())}")


bot.run(dotenv.get_key('.env','BOT_TOKEN'))

