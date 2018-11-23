package io.scalecube.cluster.leaderelection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class StateMachine {

  private Map<State, List<Consumer>> onStateHandlers = new HashMap<State, List<Consumer>>();

  private Map<State, List<Consumer>> onLeaveHandlers = new HashMap<State, List<Consumer>>();

  private final Map<State, List<State>> transitions;

  private AtomicReference<State> currentState;

  public State currentState() {
    return currentState.get();
  }

  public static final class Builder {
    private Map<State, List<State>> transitions = new HashMap<State, List<State>>();

    public Builder addTransition(State from, State to) {
      if (transitions.containsKey(from)) {
        transitions.get(from).add(to);
      } else {
        transitions.putIfAbsent(from, new ArrayList<>());
        transitions.get(from).add(to);
      }
      return this;
    }

    private State initState;

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

  public CompletableFuture<State> transition(State newState, Object obj) {
    CompletableFuture<State> futureState = new CompletableFuture<State>();

    if (!currentState.get().equals(newState)) {
      CompletableFuture<State> leaveStateFuture = new CompletableFuture<State>();
      CompletableFuture<State> onStateFuture = new CompletableFuture<State>();

      State leaveState = currentState.get();


      if (allowed().contains(newState)) {
        leaveStateFuture.supplyAsync(() -> {
          if (onLeaveHandlers.get(leaveState) != null) {
            System.out.println(onLeaveHandlers.get(leaveState));
            onLeaveHandlers.get(leaveState).forEach(action -> action.accept(obj));
          }
          leaveStateFuture.complete(leaveState);
          return leaveState;
        });

        leaveStateFuture.whenComplete((success, error) -> {
          onStateFuture.supplyAsync(() -> {
            this.currentState.set(newState);
            if (onStateHandlers.get(newState) != null) {
              onStateHandlers.get(newState).forEach(action -> action.accept(obj));
            }
            return currentState.get();
          });
        });

      } else {
        futureState.completeExceptionally(
            new IllegalStateException("not allowed tranistion from: " + currentState.get() + " to: " + newState));
      }
    }
    return futureState;
  }

  public List<Enum> allowed() {
    if (transitions.containsKey(currentState.get())) {
      return Collections.unmodifiableList(transitions.get(currentState.get()));
    }
    return Collections.EMPTY_LIST;
  }

  public void beforeExit(State state, Consumer consumer) {
    if (onLeaveHandlers.containsKey(state)) {
      onLeaveHandlers.get(state).add(consumer);
    } else {
      onLeaveHandlers.put(state, new ArrayList());
      onLeaveHandlers.get(state).add(consumer);
    }

  }

  public StateMachine on(State state, Consumer consumer) {
    if (onStateHandlers.containsKey(state)) {
      onStateHandlers.get(state).add(consumer);
    } else {
      onStateHandlers.put(state, new ArrayList());
      onStateHandlers.get(state).add(consumer);
    }

    return this;
  }


}
