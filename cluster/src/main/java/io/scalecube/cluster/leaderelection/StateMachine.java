package io.scalecube.cluster.leaderelection;

import io.scalecube.cluster.leaderelection.api.State;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.TopicProcessor;

public class StateMachine {

  private static final Logger LOGGER = LoggerFactory.getLogger(StateMachine.class);

  private final Map<State, List<State>> transitions;

  private TopicProcessor<State> onStateHandlers = TopicProcessor.create();

  private AtomicReference<State> currentState;

  public State currentState() {
    return currentState.get();
  }

  public static final class Builder {

    private State initState;
    private Map<State, List<State>> transitions = new HashMap<State, List<State>>();

    /**
     * Add allowed state transition.
     *
     * @param from transition the state is allowed.
     * @param to transition the state is allowed.
     * @return Builder.
     */
    public Builder addTransition(State from, State to) {
      if (transitions.containsKey(from)) {
        transitions.get(from).add(to);
      } else {
        transitions.putIfAbsent(from, new ArrayList<>());
        transitions.get(from).add(to);
      }
      return this;
    }

    public Builder init(State state) {
      this.initState = state;
      return this;
    }

    public StateMachine build() {
      return new StateMachine(initState, transitions);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private StateMachine(State init, Map<State, List<State>> transitions) {
    this.transitions = Collections.unmodifiableMap(transitions);
    this.currentState = new AtomicReference<State>(init);
  }

  /**
   * transition to new given allowed state.
   *
   * @param newState to transition to if allowed.
   * @throws IllegalStateException if requested state is not allowed.
   */
  public void transition(State newState) {
    if (!currentState.get().equals(newState)) {
      if (allowed().contains(newState)) {
        LOGGER.info("start transition to {}", newState);
        currentState.set(newState);
        onStateHandlers.onNext(newState);
      } else {
        throw new IllegalStateException(
            "not allowed tranistion from: " + currentState.get() + " to: " + newState);
      }
    } else {
      LOGGER.warn("no transition was done - already in state {}", newState);
    }
  }

  /**
   * return allowed states to transition to from the given current state.
   *
   * @return list of allowed states.
   */
  public List<Enum> allowed() {
    if (transitions.containsKey(currentState.get())) {
      return Collections.unmodifiableList(transitions.get(currentState.get()));
    }
    return Collections.EMPTY_LIST;
  }

  /**
   * subscribe an consumer to be executed in a given state.
   *
   * @param state to execute consumer on.
   * @param consumer function that will be executed.
   * @return StateMachine configured.
   */
  public StateMachine on(final State state, Consumer consumer) {
    onStateHandlers
        .filter(p -> p.equals(state))
        .doOnNext(
            s -> {
              consumer.accept(s);
            })
        .subscribe();
    return this;
  }
}
