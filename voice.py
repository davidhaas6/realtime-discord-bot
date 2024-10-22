import asyncio
import audioop
from dataclasses import dataclass
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
connection_data = {}
audio_handler = DiscordAudioHandler()

@dataclass
class ServerContext:
	vc: discord.VoiceClient
	ai_client: RealtimeClient
	msg_handler: asyncio.Task


@bot.command()
async def join(ctx: discord.ApplicationContext):
	voice = ctx.author.voice
	guild_id = ctx.guild.id
	if not voice:
		await ctx.respond("User isn't in a voice channel")
		return
	
	
	vc = await voice.channel.connect()
	ai_client, msg_handler = await start_realtime(vc, guild_id)
	vc.start_recording(
		discord.sinks.WaveSink(),
		stop_record_callback,
		ctx.channel,
		vc,
		ai_client,
		ctx.guild.id,
		sync_start=True,
	)

	connection_data.update({guild_id: ServerContext(vc, ai_client, msg_handler)})
	await ctx.respond("Started recording!")


@bot.command()
async def leave(ctx: discord.ApplicationContext):
	if ctx.guild.id in connection_data: 
		vc = connection_data[ctx.guild.id].vc
		vc.stop_recording() 
		del connection_data[ctx.guild.id]
		await ctx.delete() 
	else:
		await ctx.respond("I am currently not recording here.")  


async def start_realtime(vc: discord.VoiceClient, guild_id: int):
	def wrapped_audio_cbk(audio):
		audio_callback(audio, vc)
	
	client = RealtimeClient(
		api_key=dotenv.get_key('.env','OPENAI_API_KEY'),
		on_text_delta=lambda text: print(f"\nAssistant: {text}", end="", flush=True),
		on_audio_delta=wrapped_audio_cbk,
		instructions="You are a helpful AI assistant with an operatic flair. You ♪ SING LOOOOUDLY ♪  whenever you talk or perform a task as you always wish you were performing in the OPERAAAAAAAA…. ♪♪ "
		# instructions="You are a long-time smoker who speaks with a rasp and have a hacking cough that interrupts your speech every few words or so. You are employed as a helpful assistant and will do your best to work through your condition to provide friendly assistance as required. Your voice is hoarse and raspy."
		# instructions="You're, like, totally from Southern California. You say 'like' frequently, end sentences with 'you know?' or 'right?', and use words like 'totally,' 'literally,' and 'awesome' often. Raise your intonation at the end of sentences as if asking a question. Speak with a laid-back, beachy vibe and use SoCal slang."
	)
	await client.connect()
	task = asyncio.create_task(client.handle_messages())

	return client, task


def audio_callback(audio: bytes, vc: discord.VoiceClient):
	audio_handler.vc = vc
	print("Playing audio from AI")
	audio_handler.play_audio(audio)


def update_header(audio_file):
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
	logger.info(f"Received audio from users")

	files = []
	for user_id, audio in list(sink.audio_data.items())[0:1]:
		files.append(discord.File(audio.file, f"{user_id}.{sink.encoding}"))
		sink.format_audio(audio)
		_, audio_data = update_header(audio.file)
		await chat_client.send_audio(audio_data)
		logger.info("Sent audio to OpenAI RealTime API")
		
	await channel.send(f"Finished recording audio", files=files)  # Send a message with the accumulated files.
	
	await asyncio.wait_for(asyncio.sleep(15), timeout=30.0)  # Simulating waiting for a response

	await sink.vc.disconnect()  # Disconnect from the voice channel.
	await chat_client.close()  # TODO: move this eventually to have multi-turn conversations
	if guild_id in connection_data:
		connection_data[guild_id].msg_handler.cancel()


bot.run(dotenv.get_key('.env','BOT_TOKEN'))

