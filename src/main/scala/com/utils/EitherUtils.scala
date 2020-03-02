package com.utils

object EitherUtils {
  implicit class ImprovedEither[A, B](e: Either[A, B]) {
    def map[X, Y](fa: A => X, fb: B => Y): Either[X, Y] =
      e.fold(a => Left(fa(a)), b => Right(fb(b)))
  }
}
