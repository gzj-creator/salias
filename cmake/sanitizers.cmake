if(SALIAS_ENABLE_ASAN_UBSAN AND SALIAS_ENABLE_TSAN)
  message(FATAL_ERROR "ASan/UBSan and TSan cannot be enabled in the same build.")
endif()

function(salias_enable_sanitizers target)
  # A static library does not carry its link dependencies into an executable
  # unless they are part of the target's interface.  Sanitizer-instrumented
  # static libraries therefore have to export both the instrumentation compile
  # flags and the runtime link flags to installed consumers.  Executables keep
  # these flags private so that a consumer only gets them through the library
  # it links.
  get_target_property(target_type "${target}" TYPE)
  if(target_type STREQUAL "STATIC_LIBRARY")
    set(salias_sanitizer_scope PUBLIC)
  else()
    set(salias_sanitizer_scope PRIVATE)
  endif()

  if(SALIAS_ENABLE_ASAN_UBSAN)
    target_compile_options(
      ${target} ${salias_sanitizer_scope}
      -fsanitize=address,undefined
      -fno-omit-frame-pointer
    )
    target_link_options(
      ${target} ${salias_sanitizer_scope}
      -fsanitize=address,undefined
      -fno-omit-frame-pointer
    )
  elseif(SALIAS_ENABLE_TSAN)
    target_compile_options(
      ${target} ${salias_sanitizer_scope}
      -fsanitize=thread
      -fno-omit-frame-pointer
    )
    target_link_options(
      ${target} ${salias_sanitizer_scope}
      -fsanitize=thread
      -fno-omit-frame-pointer
    )
  endif()
endfunction()
