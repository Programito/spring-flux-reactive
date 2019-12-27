package com.progra.springboot.reactor.app;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.progra.springboot.reactor.app.models.Comentarios;
import com.progra.springboot.reactor.app.models.Usuario;
import com.progra.springboot.reactor.app.models.UsuarioComentarios;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SpringBootApplication
public class SpringBootReactorApplication implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(SpringBootReactorApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(SpringBootReactorApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		ejemploContraPresion();
	}
	
	public void ejemploContraPresion() {
		// log traza completa del flux
		// Subscriber sobrecarga de metodo
		// limitRate manejar lotes
		Flux.range(1, 10)
			.log()
			.limitRate(2)
			.subscribe(/*new Subscriber<Integer>() {

				private Subscription s;
				private Integer limite = 2;
				private Integer consumido = 0;
				@Override
				public void onSubscribe(Subscription s) {
					this.s = s;
					// envia el numero de elementos maximo posible
					//s.request(Long.MAX_VALUE);
					
					// por lotes, evitar cuello de botella
					s.request(limite);
					
				}

				@Override
				public void onNext(Integer t) {
					log.info(t.toString());
					consumido++;
					// solicitar dos mas cuando se consumen
					if(consumido == limite) {
						consumido = 0;
						s.request(limite);
					}
					
				}

				@Override
				public void onError(Throwable t) {
					// TODO Auto-generated method stub
					
				}

				@Override
				public void onComplete() {
					// TODO Auto-generated method stub
					
				}
			}*/);
	}
	
	public void ejemploIntervalDesdeCreate() {
		Flux.create(emitter -> {
			Timer timer = new Timer();
			// tarea, inicio, periodo en ms
			timer.schedule(new TimerTask() {
				
				private Integer contador = 0;
				@Override
				public void run() {
					emitter.next(++contador);
					// cancelar timer y el flujo
					if(contador == 10) {
						timer.cancel();
						emitter.complete();
					}
					if( contador == 5) {
						timer.cancel();
						emitter.error(new InterruptedException("Error, se ha detenido el flux en 5!"));
						
					}
				}
			}, 1000, 1000);
		})
//		.doOnNext(next -> log.info(next.toString()))
//		.doOnComplete(()->log.info("Hemos terminado"))
//		.subscribe();
		
		.subscribe(next -> log.info(next.toString()),
				error -> log.error(error.getMessage()),
				() -> log.info("Hemos terminado"));
	}
	
	public void ejemploIntervalInfinito() throws InterruptedException {
		
		CountDownLatch latch= new CountDownLatch(1);
		
		// retry si falla vuelve a lanzar el flujo
		// doOnTerminate(()->latch.countDown())
		Flux.interval(Duration.ofSeconds(1))
			.doOnTerminate(latch::countDown)
			.flatMap(i -> {
				if( i>=5) {
					return Flux.error(new InterruptedException("Solo hasta 5!"));
				}
				return Flux.just(i);
			})
			.map(i -> "Hola " + i)
//			.doOnNext(s-> log.info(s))
			.retry(2)
			.subscribe(s-> log.info(s),e -> log.error(e.getMessage()));
		
		latch.await();
	}
	
	public void ejemploDelayElements() throws InterruptedException {
		//blockLast subscribe al flujo con bloqueo
		Flux<Integer> rango = Flux.range(1, 12)
				.delayElements(Duration.ofSeconds(1))
				.doOnNext(i -> log.info(i.toString()));
		rango.blockLast();
		
//		rango.subscribe();
//		Thread.sleep(13000);
		
	}
	
	public void ejemploInterval() {
		//blockLast subscribe al flujo con bloqueo
		Flux<Integer> rango = Flux.range(1, 12);
		Flux<Long> retraso = Flux.interval(Duration.ofSeconds(1));
		rango.zipWith(retraso,(ra,re) -> ra)
			.doOnNext(i -> log.info(i.toString()))
			.blockLast();
	}
	
	// rangos en flujos
	public void ejemploZipWithRangos() {
		Flux<Integer> rangos= Flux.range(0, 4);
		Flux.just(1,2,3,4)
			.map(i -> (i*2))
			.zipWith(rangos, (uno,dos) -> String.format("Primer Flux: %d, Segundo Flux: %d", uno, dos))
			.subscribe(texto->log.info(texto));
	}
	
	// unir streams
	public void ejemploUsuarioComentariosZipWithForma2() {
		Mono<Usuario> usuarioMono = Mono.fromCallable(() -> new Usuario("John","Doe"));
		
		Mono<Comentarios> comentariosUsuarioMono = Mono.fromCallable(()->{
			Comentarios comentarios = new Comentarios();
			comentarios.addComentario("Comentario 1");
			comentarios.addComentario("Comentario 2");
			comentarios.addComentario("Comentario 3");
			return comentarios;
		});
		
		// zipwith con una entrada crea una tupla 
		Mono<UsuarioComentarios> usuarioConComentarios =usuarioMono
						.zipWith(comentariosUsuarioMono)
						.map(tuple->{
							Usuario u = tuple.getT1();
							Comentarios c = tuple.getT2();
							return new UsuarioComentarios(u, c);
						});
				
		usuarioConComentarios.subscribe(uc->log.info(uc.toString()));
	}
	
	// unir streams
	public void ejemploUsuarioComentariosZipWith() {
		Mono<Usuario> usuarioMono = Mono.fromCallable(() -> new Usuario("John","Doe"));
		
		Mono<Comentarios> comentariosUsuarioMono = Mono.fromCallable(()->{
			Comentarios comentarios = new Comentarios();
			comentarios.addComentario("Comentario 1");
			comentarios.addComentario("Comentario 2");
			comentarios.addComentario("Comentario 3");
			return comentarios;
		});
		
		//combinar los dos flujos
		Mono<UsuarioComentarios> usuarioConComentarios =usuarioMono.zipWith(comentariosUsuarioMono,(usuario,comentario)->new UsuarioComentarios(usuario, comentario));
		usuarioConComentarios.subscribe(uc->log.info(uc.toString()));
	}
	
	
	// unir streams
	public void ejemploUsuarioComentariosFlatMap() {
		Mono<Usuario> usuarioMono = Mono.fromCallable(() -> new Usuario("John","Doe"));
		
		Mono<Comentarios> comentariosUsuarioMono = Mono.fromCallable(()->{
			Comentarios comentarios = new Comentarios();
			comentarios.addComentario("Comentario 1");
			comentarios.addComentario("Comentario 2");
			comentarios.addComentario("Comentario 3");
			return comentarios;
		});
		
		usuarioMono.flatMap(u-> comentariosUsuarioMono.map(c->new UsuarioComentarios(u, c)))
			.subscribe(uc->log.info(uc.toString()));
	}
	
	// convertir a un mono
	public void ejemploCollectList() throws Exception {

		List<Usuario> usuariosList = new ArrayList<>();
		usuariosList.add(new Usuario("Andres","Ferran"));
		usuariosList.add(new Usuario("Pedro","Ramudo"));
		usuariosList.add(new Usuario("Maria","Perez"));
		usuariosList.add(new Usuario("Diego","Martorell"));
		usuariosList.add(new Usuario("Juan","Bosh"));
		usuariosList.add(new Usuario("Bruce","Lee"));
		usuariosList.add(new Usuario("Bruce","Willis"));

		// mono de solo objeto
		// lista completa la recorremos
		Flux.fromIterable(usuariosList)
			.collectList()
			.subscribe(lista -> {
				lista.forEach(item -> log.info(item.toString()));
			});
	}
	
	public void ejemploToString() throws Exception {

		List<Usuario> usuariosList = new ArrayList<>();
		usuariosList.add(new Usuario("Andres","Ferran"));
		usuariosList.add(new Usuario("Pedro","Ramudo"));
		usuariosList.add(new Usuario("Maria","Perez"));
		usuariosList.add(new Usuario("Diego","Martorell"));
		usuariosList.add(new Usuario("Juan","Bosh"));
		usuariosList.add(new Usuario("Bruce","Lee"));
		usuariosList.add(new Usuario("Bruce","Willis"));

		Flux.fromIterable(usuariosList)
				.map(usuario -> usuario.getNombre().toUpperCase().concat(" ").concat(usuario.getApellido().toUpperCase()))
				.flatMap(nombre -> {
					if(nombre.contains("bruce".toUpperCase())) {
						return Mono.just(nombre);
					}else {
						// no emite nada
						return Mono.empty();
					}
				})
				.map(nombre -> {
					return nombre.toLowerCase();
				}).subscribe(u -> log.info(u.toString()));
	}


	public void ejemploFlatMap() throws Exception {

		List<String> usuariosList = new ArrayList<>();
		usuariosList.add("Andres Ferran");
		usuariosList.add("Pedro Ramudo");
		usuariosList.add("Maria Perez");
		usuariosList.add("Diego Martorell");
		usuariosList.add("Juan Bosh");
		usuariosList.add("Bruce Lee");
		usuariosList.add("Bruce Willis");

		Flux.fromIterable(usuariosList)
				.map(nombre -> new Usuario(nombre.split(" ")[0], nombre.split(" ")[1]))
				.flatMap(usuario -> {
					if(usuario.getNombre().equalsIgnoreCase("bruce")) {
						// observable
						return Mono.just(usuario);
					}else {
						// no emite nada
						return Mono.empty();
					}
				})
				.map(usuario -> {
					String nombre = usuario.getNombre().toLowerCase();
					usuario.setNombre(nombre);
					return usuario;
				}).subscribe(u -> log.info(u.toString()));
	}

	public void ejemploIterable() throws Exception {
		// .doOnNext(elemento -> System.out.println(elemento));
		// .doOnNext(System.out::println);
		List<String> usuariosList = new ArrayList<>();
		usuariosList.add("Andres Ferran");
		usuariosList.add("Pedro Ramudo");
		usuariosList.add("Maria Perez");
		usuariosList.add("Diego Martorell");
		usuariosList.add("Juan Bosh");
		usuariosList.add("Bruce Lee");
		usuariosList.add("Bruce Willis");

		Flux<String> nombres = Flux
				.fromIterable(usuariosList); /*
												 * Flux.just("Andres Ferran", "Pedro Ramudo", "Maria Perez",
												 * "Diego Martorell", "Juan Bosh", "Bruce Lee", "Bruce Willis");
												 */
		Flux<Usuario> usuarios = nombres.map(nombre -> new Usuario(nombre.split(" ")[0], nombre.split(" ")[1]))
				.filter(usuario -> usuario.getNombre().equalsIgnoreCase("bruce")).doOnNext(usuario -> {
					if (usuario == null) {
						throw new RuntimeException("nombres no pueden ser vacíos");
					}
					System.out.println(usuario.getNombre().concat(" ").concat(usuario.getApellido()));
				}).map(usuario -> {
					String nombre = usuario.getNombre().toLowerCase();
					usuario.setNombre(nombre);
					return usuario;
				});

		// .subscribe(e -> log.info(e));
		// .subscribe(log::info);
		usuarios.subscribe(e -> log.info(e.toString()), error -> log.error(error.getMessage()), new Runnable() {

			@Override
			public void run() {
				log.info("Ha finalizado la ejecución del observable con éxito");

			}
		});
	}

}
