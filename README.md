# PageRankParalelo
Algoritmo de PageRank paralelo utilizando MPI e MPJ

Para executar o programa é necessário que tenha as seguintes dependencias instaladas

Dependencias:

    -> Java
        $ sudo apt-get default-jdk 
    -> MPI
        $ sudo apt-get libmpich-dev
    -> mpj
        - baixe o pacote no site: http://mpj-express.org/download.php
        - após completar o download execute os seguintes comandos:
            $ export MPJ_HOME=/path/para/mpj
            $ export PATH=$MPJ_HOME/bin:$PATH

Após instalar as dependencias é necessário compilar o programa, para isso use o comando na pasta raiz do projeto

    $ javac -cp .:$MPJ_HOME/lib/mpj.jar Mpi_PageRank.java

Se tudo estiver correto o programa deverá compilar e um arquivo .class será gerado.
Agora basta executar a aplicação:

    $ mpjrun.sh -np <numero processos> Mpi_PageRank

Resultado esperado:

    Top 10 URLs com melhor indice de Pagerank 
    -----------------------------------------
    |	URL 		|	Page Rank			|
    -----------------------------------------
    |	4		|	0,11673252809823267	|
    |	34		|	0,10434382238626870	|
    |	0		|	0,09357906698555896	|
    |	20		|	0,07438956930877540	|
    |	2		|	0,03582005527204208	|
    |	146		|	0,03467666892024228	|
    |   	3424	        |	0,02999266985431750	|
    |	14		|	0,01713468065462732	|
    |	6		|	0,01185502400740466	|
    |	48		|	0,01163190382513914	|

*OBS: Altere o valor das variáveis filename, outfilename, threshold e numIterations para alterar o arquivo de entrada, arquivo de saída, valor do threshold e quantidade de iterações 
realizadas, respectivamente.
