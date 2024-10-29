import numpy as np
import wave
import io
from scipy.signal import resample


def load_wav_as_array(wav_io):
    """Load a WAV file from BytesIO and return audio as a NumPy array."""
    with wave.open(wav_io, 'rb') as wav:
        params = wav.getparams()
        frames = wav.readframes(params.nframes)
        dtype = np.int16 if params.sampwidth == 2 else np.float32
        audio_array = np.frombuffer(frames, dtype=dtype)

        # Reshape if stereo (2 channels), or convert mono to stereo
        if params.nchannels == 2:
            audio_array = audio_array.reshape(-1, 2)
        else:
            audio_array = np.stack((audio_array, audio_array), axis=1)  # Duplicate for stereo

        return audio_array.astype(np.float32), params  # Convert to float32 for processing



def resample_audio(audio_array, original_rate, target_rate):
    """Resample audio to the target sample rate if necessary."""
    if original_rate == target_rate:
        return audio_array
    num_samples = int(len(audio_array) * (target_rate / original_rate))
    return resample(audio_array, num_samples)


def normalize_audio(audio_array, target_rms=0.2):
    """Normalize audio to a target RMS (Root Mean Square) loudness."""
    rms = np.sqrt(np.mean(audio_array ** 2))
    if 0 <= rms < 0.000001:  # divide by zero
        return audio_array
    normalization_factor = target_rms / rms
    return audio_array * normalization_factor


def adjust_length(track, target_length, pad_with_silence=True):
    """Match the length of the track to target_length, either by padding or looping."""
    if len(track) >= target_length:
        return track[:target_length]
    if pad_with_silence:
        padding = np.zeros((target_length - len(track), 2), dtype=np.float32)
        return np.vstack((track, padding))
    else:
        repeats = target_length // len(track) + 1
        return np.tile(track, (repeats, 1))[:target_length]


def mix_tracks(*tracks, target_rms=0.2):
    """Mix and normalize tracks with target RMS loudness."""
    max_length = max(track.shape[0] for track in tracks)
    mixed_track = np.zeros((max_length, 2), dtype=np.float32)  # Initialize for stereo

    # Adjust lengths and mix tracks
    for track in tracks:
        adjusted_track = adjust_length(track, max_length)
        mixed_track += adjusted_track / len(tracks)  # Scale down to avoid clipping

    # Final RMS normalization of the mixed track
    rms = np.sqrt(np.mean(mixed_track ** 2))
    if rms < target_rms:  # Apply gain if below target volume
        gain_factor = target_rms / rms
        mixed_track *= gain_factor

    # Soft clipping to prevent hard clipping
    mixed_track = np.tanh(mixed_track) * 32767
    return mixed_track.astype(np.int16)


def save_to_wav(audio_array, params, output_io):
    """Save a NumPy array as a WAV file to BytesIO."""
    with wave.open(output_io, 'wb') as wav_out:
        wav_out.setparams(params)
        wav_out.writeframes(audio_array.tobytes())


def main_mix_function(wav_ios, frame_rate=48000, target_rms=0.15, pad_with_silence=True):
    """Main function to mix audio from a list of BytesIO objects with improved handling."""
    tracks = []
    params = None

    for wav_io in wav_ios:
        audio_array, params = load_wav_as_array(wav_io)
        # Resample audio if needed
        if params.framerate != frame_rate:
            audio_array = resample_audio(audio_array, params.framerate, frame_rate)

        # Normalize each track to target RMS
        normalized_audio = normalize_audio(audio_array, target_rms)
        tracks.append(normalized_audio)

    # Mix tracks with target RMS normalization and length adjustment
    mixed_track = mix_tracks(*tracks, target_rms=target_rms)

    # Save to BytesIO
    output_io = io.BytesIO()
    params = params._replace(framerate=frame_rate, nchannels=2, sampwidth=2)  # Update params
    save_to_wav(mixed_track, params, output_io)
    output_io.seek(0)

    return output_io


import io
import sys
from pathlib import Path


def main():
    import argparse

    # Set up argument parser for command-line options
    parser = argparse.ArgumentParser(description="Mix multiple WAV files into one.")
    parser.add_argument("paths", nargs="+", help="Paths to the input WAV files")
    parser.add_argument("--output", default="mixed_output.wav", help="Path for the output mixed WAV file")
    parser.add_argument("--target_rate", type=int, default=44100, help="Target sample rate for the output file")
    parser.add_argument("--target_rms", type=float, default=0.2, help="Target RMS level for normalization")
    parser.add_argument("--pad_with_silence", action="store_true",
                        help="Pad shorter tracks with silence (default: False, loops shorter tracks)")

    args = parser.parse_args()

    # Verify all input paths exist
    audio_streams = []
    for path in args.paths:
        if not Path(path).is_file():
            print(f"Error: File '{path}' not found.")
            sys.exit(1)

        # Read each file and store as BytesIO
        with open(path, "rb") as f:
            audio_stream = io.BytesIO(f.read())
            audio_streams.append(audio_stream)

    # Call main_mix_function to mix audio streams
    mixed_audio_stream = main_mix_function(
        audio_streams,
        frame_rate=args.target_rate,
        target_rms=args.target_rms,
        pad_with_silence=args.pad_with_silence
    )

    # Save the result to the specified output path
    with open(args.output, "wb") as f:
        f.write(mixed_audio_stream.getbuffer())

    print(f"Mixed audio saved to '{args.output}'.")


# Call the main function if running as a script
if __name__ == "__main__":
    main()

