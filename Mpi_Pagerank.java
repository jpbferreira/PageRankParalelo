/**
* @author João Pedro Batista Ferreira e Marco Aurélio Ferreira de Sousa
*/
 
import java.util.*;
import java.io.*;
import mpi.*;

public class Mpi_Pagerank {

	/**
	 * Função responsável por ler o arquivo de entrada com a base de dados e armazenar na estrutura de dados definida para representar os links
	 * e suas referencias - (HashMap<Identificador, Lista de links/referencias>).
	 * O processo principal (rank = 0) é responsável por ler o arquivo de entrada e baseado na quantidade de processos ele divide os dados lidos 
	 * em blocos e envia-os para os outros processos para que estes possam processar os dados e armazenar na estrutura de dados.
	 * @param filename String - nome do arquivo de entrada.
	 * @param adjacency_matrix HashMap<Integer, ArrayList<Integer>> - estrutura de dados que representa os links.
	 * @param am_index ArrayList<Integer> - array que armazena o par (url, quantidade de links para a url)
	 * @param communicator Intracomm - MPI communicator - MPI.COMM_WORLD
	 */
	public static void mpi_read(String filename, HashMap<Integer, ArrayList<Integer>> adjacency_matrix, ArrayList<Integer> am_index, Intracomm communicator)
	{

        int startIndex = 0;
        int blockSize = 0;
		int rank = communicator.Rank();
		
		// processo 0 (main) lê o arquivo e envia um bloco com as entradas lidas para cada um dos outros processos
		if(rank == 0) {
			try {

				FileInputStream file = new FileInputStream(filename);
		    	DataInputStream datastr = new DataInputStream(file);
		    	BufferedReader urlreader = new BufferedReader(new InputStreamReader(datastr));
		    	String adjmatrix;
		    	
		    	while((adjmatrix = urlreader.readLine()) != null) {

					ArrayList<Integer> outLinksList = new ArrayList<Integer>();
		    		
					//Separa o identificador do link de suas referencias
		    		String[] nodelist = adjmatrix.split(" ");
		    		for(int i = 1; i < nodelist.length; i++) {
		    			outLinksList.add(Integer.valueOf(nodelist[i]));
		    		}
		    		
		    	 	adjacency_matrix.put(Integer.valueOf(nodelist[0]), outLinksList);
				}
				
		    	datastr.close();
			} catch(Exception ex) {
				System.err.println("Error: "+ ex.getMessage());
			}
			
			int totalNumberOfUrls = adjacency_matrix.size(); // numero total de entradas
			int totalRank = communicator.Size(); // numero de processos
			int numberOfDivisions = (totalNumberOfUrls) / totalRank; // numero de elementos por bloco
			int remainder = (totalNumberOfUrls) % totalRank;
		  	
		  	ArrayList<Integer> sourceUrls = new ArrayList<Integer>(adjacency_matrix.keySet());

			int initialIndex = 0;
			int j = 0;
			
			try {

	            for (int i = 0; i < totalRank; i++) {
	            	startIndex = initialIndex; 
	                      	
	                int index = 0;
	
					//Calcula o tamanho do bloco - o numero de entradas for impar, o ultimo bloco fica com menos elementos
					blockSize = remainder == 0 ? numberOfDivisions : (i < remainder) ? (numberOfDivisions + 1) : (numberOfDivisions);
	
		        	int [] t_am_index = new int[blockSize*2]; 
	                for (initialIndex = startIndex; initialIndex <= ((startIndex+blockSize)-1); initialIndex++) {
	                	
						int source = sourceUrls.get(j++);
						ArrayList<Integer> targetUrls = adjacency_matrix.get(source);
						int outdegree = targetUrls.size();
						
						// adiciona o par (sourceUrl, numLinks)
						t_am_index[index++] = source;
						t_am_index[index++] = outdegree;
	                         
	                }
	                
	                int[] am_size = new int[1];
	                am_size[0] = blockSize * 2;
	
	                if (i == 0) {
	                	for (int l = 0; l < am_size[0]; l++) {
	                		am_index.add(t_am_index[l]);
	                	}
	                } else {
	                	communicator.Send(am_size, 0, 1, MPI.INT, i, 0); // envia tamanho array am_index
	                	communicator.Send(t_am_index, 0, am_size[0], MPI.INT, i, 1); // envia conteudo array t_am_index - par(sourceUrl, numLinks)
	                }
	                
	                for(int k = 0; k < blockSize*2; k = k+2) {
	                    int source = t_am_index[k];
	                    int outdegree = t_am_index[k+1];
	                    int[] targetList = new int[outdegree];
	
	                    ArrayList<Integer> targetUrls = adjacency_matrix.get(source);
	
	                    for(int n = 0; n < outdegree; n++) {
	                    	targetList[n] = targetUrls.get(n);
	                    }
	
	                    if(i != 0) {
	                    	communicator.Send(targetList, 0, outdegree, MPI.INT, i, 2); // envia lista com os links que apontam para o sourceUrl
	                    }
	                }
		        }
            }
            catch (Exception ex) {
            	System.out.println("Error: "+ ex.getMessage());
            }
		} else {
			
			int[] am_size = new int[1];
			communicator.Recv(am_size, 0, 1, MPI.INT, 0, 0); // recebe a mensagem enviada pelo rank 0 com o tamanho do array (sourceUrl, numLinks)

			int[] t_am_index = new int[am_size[0]];
			communicator.Recv(t_am_index, 0, am_size[0], MPI.INT, 0, 1); // recebe a mensagem enviada pelo rank 0 com o array (sourceUrl, numLinks)

			for (int l = 0; l < am_size[0]; l++) {
				am_index.add(t_am_index[l]); // adiciona cada item (sourceUrl/numLinks) em ordem
			}

			int no2Urls = am_size[0];
			for(int p = 0; p < no2Urls; p=p+2) {
				
				int sourceUrl = t_am_index[p];
				int outdegree = t_am_index[p+1];
				int[] target = new int[outdegree];
				
				communicator.Recv(target, 0, outdegree, MPI.INT, 0, 2); // recebe lista enviada pelo rank 0 com os links que apontam para o sourceUrl
				ArrayList<Integer> targetUrls = new ArrayList<Integer>();

				for(int m = 0; m < outdegree; m++) {
					targetUrls.add(target[m]);
				}

				adjacency_matrix.put(sourceUrl, targetUrls); // armazena os dados de entrada na estrutura de dados
			}
		}
	}
	
	/**
	 * Função responsável por calcular o valor do PageRank para cada URL. Primeiro calcula um valor intermediário para cada pagina e atualiza baseado nos valores obtidos 
	 * pelos outros processos. Depois calcula o valor final do pagerank levando em consideração o valor de dumping e trasmite para todos os outros processos para ficarem atualizados.
	 * @param adjacency_matrix HashMap<Integer, ArrayList<Integer>> - estrutura de dados que representa os links.
	 * @param am_index ArrayList<Integer> - array que armazena o par (url, quantidade de links para a url)
	 * @param totalNumUrls int - numero total de entradas lidas do arquivo de entrada
	 * @param numIterations int - numero de iterações a serem feitas. Cada iteração aumenta a precisao do calculo de pagerank ate atingir a condição especificada pelo threshold
	 * @param threshold double - numero utilizado para determinar a quantidade de iterações feitas para calcular o pagerank
	 * @param FinalRVT double[] - array que armazena o pagerank de cada Url
	 * @param communicator Intracomm - MPI communicator (MPI.COMM_WORLD)
	 */
	public static void mpi_pagerankfunc(HashMap<Integer, ArrayList<Integer>> adjacencyMatrix, ArrayList<Integer> amIndex, 
			int totalNumUrls, int numIterations, double threshold, double[] FinalRVT, Intracomm communicator) {
		
		try {
			 
			double delta = 0.0; //atua no controle do loop
			double dangling = 0.0;
			double sum_dangling = 0.0;
			double intermediate_rank_value = 0;
			double damping_factor = 0.85;
			
			int source = 0;
			int outdegree = 0;
			int targetUrl;
			int loop = 0;  
			
			ArrayList<Integer> targetUrls = new ArrayList<Integer>(); 
			
			double [] intermediateRV = new double[totalNumUrls]; 
			double [] localRV = new double[totalNumUrls]; 
			double [] danglingArray = new double[1]; 
			double [] sumDangling = new double[1]; 
			double [] deltaArray = new double[1]; 
			deltaArray[0] = 0.0;
			
			int rank = communicator.Rank();
				
			if(rank == 0)
				System.out.println("Numero maximo de iteracoes: " + numIterations + ", Valor do Threshold: " + threshold);
			
			/* Inicia a computacao do PageRank */ 
			do { 
				
				/* Calcula o pagerank inicial e danglig value */ 
				dangling = 0.0;
				
				for (int i = 0; i < amIndex.size(); i = i+2) { 
					
					source = amIndex.get(i);
					targetUrls =  adjacencyMatrix.get(source);
					outdegree = targetUrls.size();
					
					for (int j = 0; j < outdegree; j++) {
						targetUrl = targetUrls.get(j);
						intermediate_rank_value = localRV[targetUrl] + FinalRVT[source] / (double)outdegree;
						localRV[targetUrl] = intermediate_rank_value;
					}
					
					if(outdegree == 0) {
						dangling += FinalRVT[source];
					}
				}
				
				/* Atualiza o valor do pagerank baseado nos valores de todos processos */ 
				communicator.Allreduce(localRV, 0, FinalRVT, 0, totalNumUrls, MPI.DOUBLE, MPI.SUM); 
		
				/* Atualiza o valor do dangling baseado nos valores de todos processos */ 
				danglingArray[0] = dangling;
				communicator.Allreduce(danglingArray, 0, sumDangling, 0, 1, MPI.DOUBLE, MPI.SUM); 
				sum_dangling = sumDangling[0];
				
				/* Recalcula o valor do pagerank, agora levando em consideração o fator de dumping = 0.85 */ 
				/* Processo principal (rank = 0) realiza o processamento e decide quando o loop irá parar*/ 
				if(rank == 0) {
					
					double dangling_value_per_page = sum_dangling / totalNumUrls;
					
					for (int i = 0; i < totalNumUrls; i++) {
						FinalRVT[i] += dangling_value_per_page;
						FinalRVT[i] = damping_factor * FinalRVT[i] + (1-damping_factor) * (1.0/(double)totalNumUrls);
					}
		
					deltaArray[0] = 0.0;
					
					for(int i = 0; i < totalNumUrls; i++){ 
						deltaArray[0] += Math.abs(intermediateRV[i] - FinalRVT[i]);
						intermediateRV[i] = FinalRVT[i];
					}
												
				}
				
				// envia o valor do delta atualizado para todos os processos
				communicator.Bcast(deltaArray, 0, 1, MPI.DOUBLE, 0);
				
				// envia o valor atual do pagerank para os todos os processos
				communicator.Bcast(FinalRVT, 0, totalNumUrls, MPI.DOUBLE, 0); 
				
				for (int k = 0; k < totalNumUrls; k++) {
					localRV[k] = 0.0;
				}
							
				if(rank == 0)
					System.out.println("=> Iteracao atual: " + loop + " - valor do delta: " + deltaArray[0]);
				
			} while (deltaArray[0] > threshold && ++loop < numIterations); 
			
		} catch(Exception ex) {
			System.err.println("Error: " + ex.getMessage());
		}
	}

	/**
	 * Função responsável por escrever os resultados no arquivo de saída. Recebe um hashmap com o par(sourceUrl, valor pagerank) e o ordena em ordem decrescente
	 * de valor de pagerank e escreve os 10 primeiros (maiores) no arquivo de saída e na tela do usuário.
	 * @param filename String - nome do arquivo de saída
	 * @param sortHash LinkedHashMap<Integer, Double> sortHash) - Hashmap com os resultados do calculo de pagerank (sourceUrl, pagerank)
	 */
	private static void mpi_write(String filename, LinkedHashMap<Integer, Double> sortHash) throws IOException {
		
		try {
			
			double sum_of_probabilities = 0.0;
		    for(Double val : sortHash.values()) {
		    	sum_of_probabilities += val;
		    }
		    
		    int[] keys = new int[sortHash.size()];
		    double[] values = new double[sortHash.size()];
		    int index = 0;
			
			for (Map.Entry<Integer, Double> mapEntry : sortHash.entrySet()) {

		    	keys[index] = Integer.parseInt(mapEntry.getKey().toString());
		    	values[index] = Double.parseDouble(mapEntry.getValue().toString());
		    	index++;
		    }
		
		    /* Ordena os resulatdos do calculo do pagerank */
	
		    List<Double> page_rank_list = new ArrayList<Double>(sortHash.values());
		    Collections.sort(page_rank_list);
		    ListIterator sorted_page_rank_iterator = page_rank_list.listIterator(page_rank_list.size());
		    int number_web_pages = 0;
		    int totalUrls = page_rank_list.size();
	    
		    Writer output = null;
		    File toptenurllist = new File(filename);
		    output = new BufferedWriter(new FileWriter(toptenurllist));
	     
		    output.append("\nTop 10 URLs com melhor indice de Pagerank "+"\n\n"+"---------------------------------------------------------"+"\n");
		    output.append("|\t"+"URL "+"\t\t|\t"+"Page Rank"+"\t\t\t|\n"+"---------------------------------------------------------"+"\n");
			
			System.out.println("\nTop 10 URLs com melhor indice de Pagerank "+"\n\n"+"---------------------------------------------------------"+"\n");
		    System.out.println("|\t"+"URL "+"\t\t|\t"+"Page Rank"+"\t\t\t|"+"\n"+"---------------------------------------------------------"+"\n");

			while (sorted_page_rank_iterator.hasPrevious() && number_web_pages++ < 10) {
				
				String str = sorted_page_rank_iterator.previous().toString();
		    	double pagerankop = Double.valueOf(str).doubleValue();
		  
		    	// escreve os 10 primeiros (maiores) pageranks no arquivo de saída e na tela do usuário.
		     
		    	for (int i = 0; i < totalUrls; i++) {
		    		if (values[i] == pagerankop) {	

		    			output.write("|\t" + keys[i] + "\t\t|\t" + String.format("%2.17f",pagerankop) + "\t|\n");
		    			System.out.println("|\t" + keys[i] + "\t\t|\t" + String.format("%2.17f",pagerankop) + "\t|\n");
		   			 	output.write("---------------------------------------"+"\n");
		   			 	values[i]=-1;
		   				break;
		    		}
		    	}
		    }
		    
		    try {
		    	if (output!=null) {	
		    		output.close();
		    	}
		    } catch(IOException er) {
		    	er.printStackTrace();
		    } catch(Exception ex){
			System.err.println("Error: " + ex.getMessage());
			}

		} catch(Exception ex){
			System.err.println("Error: " + ex.getMessage());
		}
	}


    public static void main(String args[]) throws Exception 
    {
		/* 
		* Definição da estrura de dados utilizada para representar os links. 
		* Cada link possui um identificador e uma lista de links que o referenciam
		*/
    	HashMap <Integer,ArrayList<Integer>> adjacencyMatrix = new HashMap<Integer, ArrayList<Integer>>();  
    	ArrayList<Integer> am_index = new ArrayList<Integer>();
    	LinkedHashMap<Integer, Double> pagerankvalues = new LinkedHashMap<Integer, Double>();
    	int totalNumUrls; 

        int numUrls; 
        String filename = "pagerank10k.txt"; // base dados utilizada  
        String outfilename ="output.txt"; // nome do arquivo com os resultados gerados
        double threshold = 0.001; // valor do threshold
		int numIterations = 10; 
       
		int rank;
        
		long comp_start = 0;
		long comp_end = 0;
        
        try {
        
		    /* Inicio da paralelização utilizando MPI */ 
		    MPI.Init(args); 
		    rank = MPI.COMM_WORLD.Rank();
			
			if (rank == 0)
				comp_start=System.currentTimeMillis();
		    mpi_read(filename, adjacencyMatrix, am_index, MPI.COMM_WORLD);
		    	        
		    /* Set totalNumUrls */
		    numUrls = am_index.size()/2;
			int numURL[] = new int[1];
			numURL[0] = numUrls;
			int totalNumUrl[] = new int[1];
			MPI.COMM_WORLD.Allreduce(numURL, 0, totalNumUrl, 0, 1, MPI.INT, MPI.SUM);
			totalNumUrls = totalNumUrl[0];
			double FinalRVT[] = new double[totalNumUrls];

			for (int k = 0; k < totalNumUrls;k++) {
				FinalRVT[k] = 1.0/totalNumUrls;
			}      
			 
			try {
				/* Inicia a computacao do PageRank com MPI */
				mpi_pagerankfunc(adjacencyMatrix, am_index, totalNumUrls, numIterations, threshold, FinalRVT, MPI.COMM_WORLD);
			}
			catch(Exception ex) {
				System.err.println("Error: " + ex.getMessage());
			}
				
		    /* Chama a função que E=escreve os resultados no arquivo de saída */ 
			if (rank == 0) {
				for (int i = 0; i < FinalRVT.length; i++) {
					pagerankvalues.put(i, FinalRVT[i]);
				}
				
				mpi_write(outfilename, pagerankvalues);
				comp_end = System.currentTimeMillis() - comp_start;
				System.out.println("\nTempo total de computação:" + comp_end + " (ms) ");
			}
			 
			 MPI.Finalize(); 
		} catch(NumberFormatException ex) {
        	System.out.println(ex.getMessage());
        }
    }
}
