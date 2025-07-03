from tqdm import tqdm
import time

jogos = ["jogo1", "jogo2", "jogo3", "jogo4"]

for jogo in tqdm(jogos, desc="Consultando API", ncols=100):
    # simula chamada à API
    time.sleep(1)
